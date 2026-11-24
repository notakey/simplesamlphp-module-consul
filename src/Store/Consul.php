<?php

declare(strict_types=1);

namespace SimpleSAML\Module\consul\Store;

use Consul\Client;
use Consul\Exception\ClientException;
use Consul\Services\KV;
use SimpleSAML\Configuration;
use SimpleSAML\Error;
use SimpleSAML\Logger;
use SimpleSAML\Store\StoreInterface;

/**
 * Consul KV backed data store.
 *
 * Use as 'store.type' => 'SimpleSAML\Module\consul\Store\Consul' in config.php.
 */
class Consul implements StoreInterface
{
    /**
     * The prefix we should use for our keys.
     */
    private string $prefix;

    /**
     * Allow usage of multikey values.
     */
    private bool $multikey = true;

    /**
     * KV connection.
     */
    private KV $conn;

    /**
     * Initialize the Consul datastore.
     */
    public function __construct()
    {
        $consulconfig = Configuration::getConfig('module_consul.php');

        $this->prefix = $consulconfig->getOptionalString('kv_prefix', 'sso');
        $this->multikey = $consulconfig->getOptionalBoolean('multikey', false);

        $url = $consulconfig->getOptionalString('kv_url', 'http://localhost:8500');

        try {
            $this->conn = new KV(new Client(['base_uri' => $url]));
        } catch (\Exception $ex) {
            throw new Error\Exception("Cannot initialize KV store, verify that kv_url is configured", 8764);
        }
    }

    /**
     * Retrieve a value from the datastore.
     *
     * @param string $type  The datatype.
     * @param string $key  The key.
     * @return mixed|null  The value.
     */
    public function get(string $type, string $key): mixed
    {
        return $this->getValue($type, $key);
    }

    /**
     * Retrieve a value, or all values of a type when $key is null.
     */
    private function getValue(string $type, ?string $key): mixed
    {
        if (is_null($key)) {
            // get all values
            $t = [];
            $kv_ix = $this->get_all($type);
            foreach ($kv_ix as $k => $ix) {
                $t[$k] = $this->getValue($type, $ix);
            }

            return $t;
        }

        $getkey = $this->getRequestPath($type, $key);
        try {
            $val = $this->conn->get($getkey, ['raw' => true]);
        } catch (ClientException $ex) {
            return null;
        }

        Logger::debug('Consul: Fetch ' . $getkey);
        // if($val->getStatusCode() == 404){
        //     return null;
        // }

        return $this->decodeValue($type, $key, $val->getBody());
    }

    private function get_all($type = "")
    {
        $t = [];

        $path = $this->getRequestPath($type);
        try {
            $val = $this->conn->get($path, ['keys' => true]);
        } catch (ClientException $ex) {
            return $t;
        }

        $kv_ix = json_decode($val->getBody(), true);

        if (!is_array($kv_ix)) {
            throw new Error\Exception("Failed index for type $type", 8767);
        }

        // TODO possible OOM exception here, need custom exception and paged loading
        foreach ($kv_ix as $ix => $v) {
            $kv_ix[$ix] = str_replace($path, '', $v);
            Logger::debug('Consul: get all loaded ' . $path . ': ' . $ix . ', ' . $kv_ix[$ix]);
        }

        return $kv_ix;
    }

    /**
     * Save a value to the datastore.
     *
     * @param string $type  The datatype.
     * @param string $key  The key.
     * @param mixed $value  The value.
     * @param int|NULL $expire  The expiration time (unix timestamp), or NULL if it never expires.
     */
    public function set(string $type, string $key, mixed $value, ?int $expire = null): void
    {
        $payload = $this->serialize($value);
        Logger::debug('Consul: Store ' . $type . '/' . $key . ' serialized ' . strlen($payload) . 'B');

        $this->setScalar($type, $key, $payload, $expire, 0);
    }

    private function getNestedKey($key)
    {
        return "$key-data";
    }

    private function setScalar($type, $key, $value, $expire, $serial = 0)
    {
        $esize = strlen($value);
        $multikey = 0;
        $mthold = 1024 * 400;
        $hash = md5($value);
        $old_hash = '';
        $storekey = $this->getRequestPath($type, $key);


        if ($esize > $mthold * 10 || (!$this->multikey && $esize > $mthold)) {
            Logger::error('Consul: setScalar ' . $this->getRequestPath($type, $key) . ' exceeds limit (' . $esize . ' vs. ' . ($mthold * 50) . '), key deleted');
            $this->delete($type, $key);
            throw new Error\Exception("Playload for key " . $this->getRequestPath($type, $key) . " exceeds limit", 8765);
        }

        if ($this->multikey && $esize > $mthold) {
            // multi value key
            // $this->delete($type, $key);
            $oldvalue = null;
            try {
                $oldvalue = $this->conn->get($this->getRequestPath($type, $key), ['raw' => true]);
            } catch (ClientException $ex) {
                // intentionally blank
            }

            if ($oldvalue != null) {
                $old = $oldvalue->json();
                $old_hash = $old['hash'];
                Logger::debug('Consul: Got ' . $storekey . ' old hash ' . $old_hash);
            }

            $value = base64_encode($value);
            $chunks = str_split($value, $mthold);
            foreach ($chunks as $ix => $chunk) {
                $subkey = $this->mergePath($this->mergePath($type, $this->getNestedKey($key)), $hash);
                Logger::debug('Consul: Store ' . $subkey . ' multi key ' . strlen($chunk) . 'B');
                $this->setScalar($subkey, strval($ix), $chunk, $expire, 1);
            }

            $value = count($chunks);
            $multikey = 1;
            $serial = 1;
        }

        $encval = $this->encodeValue($key, $value, $expire, $multikey, $serial, $hash, $old_hash);

        Logger::debug('Consul: Store ' . $storekey . ' ' . $esize . 'B');

        try {
            $retval = $this->conn->put($storekey, $encval);
        } catch (ClientException $ex) {
            Logger::error('Consul: Set ' . $storekey . " failure");
            throw new Error\Exception("KV Store set error.", 8596);
        }

        if ($multikey == 1 && $old_hash != '') {
            $delkey = $this->mergePath($this->mergePath($type, $this->getNestedKey($key)), $old_hash);
            Logger::debug('Consul: Delete old hash ' . $delkey);

            try {
                $this->conn->delete($delkey, ['recurse' => true]);
            } catch (ClientException $ex) {
                Logger::error('Consul: Delete ' . $delkey . " failure, recurse = true");
                throw new Error\Exception("KV Store delete error.", 8597);
            }
        }

        return $retval;
    }

    /**
     * Delete a value from the datastore.
     *
     * @param string $type  The datatype.
     * @param string $key  The key.
     */
    public function delete(string $type, string $key): void
    {
        $this->deleteValue($type, $key);
    }

    /**
     * Delete a value, or all values of a type when $key is null.
     */
    private function deleteValue(string $type, ?string $key): mixed
    {
        if (is_null($key)) {
            // delete all values
            $kv_ix = $this->get_all($type);
            foreach ($kv_ix as $ix) {
                $this->deleteValue($type, $ix);
            }

            try {
                return $this->conn->delete($this->getRequestPath($type));
            } catch (ClientException $ex) {
                Logger::error('Consul: Delete ' . $this->getRequestPath($type) . " failure");
                throw new Error\Exception("KV Store delete error.", 8599);
            }
        }

        Logger::debug('Consul: Delete ' . $type . '/' . $key);

        try {
            $this->conn->delete($this->getRequestPath($type, $this->getNestedKey($key)), ['recurse' => true]);
        } catch (ClientException $ex) {
            // intentionally blank
        }

        try {
            return $this->conn->delete($this->getRequestPath($type, $key));
        } catch (ClientException $ex) {
            Logger::error('Consul: Delete ' . $this->getRequestPath($type, $key) . " failure");
            throw new Error\Exception("KV Store delete error.", 8599);
        }
    }

    /**
     * Clean the key-value table of expired entries.
     */
    public function cleanKVStore(): void
    {
        // No namespace here
        Logger::debug('store.consul: Cleaning key-value store.');

        $kvarr = $this->get_all("");
        $delc = 0;
        foreach ($kvarr as $t) {
            if (strpos($t, '/') === false) {
                continue;
            }

            [$type, $key] = explode("/", $t, 2);

            try {
                $val = $this->conn->get($this->getRequestPath($type, $key), ['raw' => true]);
                $v = json_decode($val->getBody(), true);
                if ($v['expires'] > 0 && $v['expires'] < time()) {
                    $this->delete($type, $key);
                    $delc++;
                }
            } catch (ClientException $ex) {
                Logger::debug('store.consul: cleanKVStore error for ' . $this->getRequestPath($type, $key) . ': ' . $ex->getMessage() . '.');
                continue;
            } catch (\Exception $ex) {
                Logger::debug('store.consul: cleanKVStore error: ' . $ex->getMessage() . '.');
                continue;
            }
        }

        // No namespace here
        Logger::debug("store.consul: Cleanup complete, $delc items removed");
    }

    public function getPrefix(): string
    {
        return $this->mergePath($this->prefix, "v2");
    }

    public function mergePath(string $root, ?string $key): string
    {
        if (substr($root, -1, 1) == '/') {
            $root = substr($root, 0, -1);
        }

        if (is_null($key) || $key === "") {
            return $root;
        }

        if (substr($key, -1, 1) == '/') {
            $key = substr($key, 0, -1);
        }

        if (substr($key, 0, 1) == '/') {
            $key = substr($key, 1);
        }

        return "$root/$key";
    }

    private function getRequestPath(string $root = "", ?string $key = null): string
    {

        return $this->mergePath($this->mergePath($this->getPrefix(), $root), $key);
    }

    private function serialize(mixed $value): string
    {
        return serialize($value);
    }

    private function unserialize(string $value): mixed
    {
        return unserialize($value);
    }

    private function encodeValue($key, $val, $expire = 0, $multikey = 0, $serialData = 0, $hash = '', $old_hash = '')
    {

        $v = [
            'created_at_str' => date("c"),
            'keyname' => $key,
            'expires' => $expire,
            'hash' => '',
            'multi' => $multikey,
            'serial' => $serialData,
            'payload' => $val
        ];

        if ($hash != '') {
            $v['hash'] = $hash;
        }

        if ($old_hash != '') {
            $v['old_hash'] = $old_hash;
        }

        return json_encode($v);
    }

    private function decodeValue($type, $key, $val)
    {
        $pl = json_decode($val, true);

        if ($pl['expires'] > 0 && $pl['expires'] < time()) {
            return null;
        }

        $payload = $pl['payload'];

        if (@$pl['serial'] == 0) {
            $payload = $this->unserialize($payload);
        }

        if (!$this->multikey && @$pl['multi'] == 1) {
            // Multikey is not allowed, but value stored in this format
            return null;
        }

        if (@$pl['multi'] == 1) {
            $chunks = $this->getValue($this->mergePath($this->mergePath($type, $this->getNestedKey($key)), $pl['hash']), null);
            $ccount = intval($payload);

            $payload = "";
            for ($i = 0; $i < $ccount; $i++) {
                $payload .= base64_decode($chunks[$i]);
            }

            if (md5($payload) != $pl['hash']) {
                Logger::error('Consul: decodeValue ' . $this->getRequestPath($type, $key) . ' checksum mismatch (' . $pl['hash'] . '), key deleted ');
                $this->delete($type, $key);
                throw new Error\Exception("Checksum error for stored data", 8798);
            }

            $payload = $this->unserialize($payload);
        }

        return $payload;
    }
}
