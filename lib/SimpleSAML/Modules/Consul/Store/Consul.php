<?php

namespace SimpleSAML\Modules\Consul\Store;

use SensioLabs\Consul\ServiceFactory;
use SensioLabs\Consul\Services;
use SimpleSAML\Logger;
use SimpleSAML\Store;

class Consul extends Store
{
    /**
     * The prefix we should use for our tables.
     *
     * @var string
     */
    private $prefix;

    /**
     * Allow usage of multikey values.
     *
     * @var bool
     */
    private $multikey = true;

    /**
     * Service Factory
     */
    private $sf;

    /**
     * KV connection.
     */
    private $conn;

    /**
     * Initialize the SQL datastore.
     */
    protected function __construct()
    {
        $consulconfig = \SimpleSAML\Configuration::getConfig('module_consul.php');

        $this->prefix = $consulconfig->getString('kv_prefix', 'sso');
        $this->multikey = $consulconfig->getBoolean('multikey', true);

        $url = $consulconfig->getString('kv_url', 'http://localhost:8500');

        $this->sf = new \SensioLabs\Consul\ServiceFactory(['base_uri' => $url]);

        try {
            $this->conn = $this->sf->get('kv');
        } catch (Exception $ex) {
            throw new \SimpleSAML_Error_Exception("Cannot initialize KV store, verify that kv_url is configured", 8764);
        }
    }

    /**
     * Retrieve a value from the datastore.
     *
     * @param string $type  The datatype.
     * @param string $key  The key.
     * @return mixed|NULL  The value.
     */
    public function get($type, $key)
    {
        assert('is_string($type)');
        assert('is_string($key) || is_null($key)');

        if (is_null($key)) {
            // get all values
            $t = array();
            $kv_ix = $this->get_all($type);
            foreach ($kv_ix as $k => $ix) {
                $t[$k] = $this->get($type, $ix);
            }


            return $t;
        }

        $getkey = $this->getRequestPath($type, $key);
        try {
            $val = $this->conn->get($getkey, ['raw' => true]);
        } catch (\SensioLabs\Consul\Exception\ClientException $ex) {
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
        $t = array();

        $path = $this->getRequestPath($type);
        try {
            $val = $this->conn->get($path, ['keys' => true]);
        } catch (\SensioLabs\Consul\Exception\ClientException $ex) {
            return $t;
        }

        $kv_ix = json_decode($val->getBody(), true);

        if (!is_array($kv_ix)) {
            throw new \SimpleSAML_Error_Exception("Failed index for type $type", 8767);
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
    public function set($type, $key, $value, $expire = NULL)
    {
        assert('is_string($type)');
        assert('is_string($key)');
        assert('is_null($expire) || (is_int($expire) && $expire > 2592000)');

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
            \SimpleSAML\Logger::error('Consul: setScalar ' . $this->getRequestPath($type, $key) . ' exceeds limit (' . $esize . ' vs. ' . ($mthold * 50) . '), key deleted');
            $this->delete($type, $key);
            throw new \SimpleSAML_Error_Exception("Playload for key " . $this->getRequestPath($type, $key) . " exceeds limit", 8765);
        }

        if ($this->multikey && $esize > $mthold) {
            // multi value key
            // $this->delete($type, $key);
            $oldvalue = null;
            try {
                $oldvalue = $this->conn->get($this->getRequestPath($type, $key), ['raw' => true]);
            } catch (\SensioLabs\Consul\Exception\ClientException $ex) {
                // intentionally blank
            }

            if ($oldvalue != null) {
                $old = $oldvalue->json();
                $old_hash = $old['hash'];
                Logger::debug('Consul: Got ' . $storekey . ' old hash ' . $old_hash);
            }

            $value = base64_encode($value);
            $chunks = str_split($value, $mthold);
            while (list($ix, $chunk) = each($chunks)) {
                $subkey = $this->mergePath($this->mergePath($type, $this->getNestedKey($key)), $hash);
                Logger::debug('Consul: Store ' . $subkey . ' multi key ' . strlen($chunk) . 'B');
                $this->setScalar($subkey, strval($ix), $chunk, $expire, 1);
            }

            $value = count($chunks);
            $multikey = 1;
            $serial = 1;
        }

        $encval = $this->encodeValue($key, $value, $expire, $multikey, $serial, $hash, $old_hash);

        \SimpleSAML\Logger::debug('Consul: Store ' . $storekey . ' ' . $esize . 'B');
        $retval = $this->conn->put($storekey, $encval);

        if ($multikey == 1 && $old_hash != '') {
            $delkey = $this->mergePath($this->mergePath($type, $this->getNestedKey($key)), $old_hash);
            Logger::debug('Consul: Delete old hash ' . $delkey);
            $this->conn->delete($delkey, array('recurse' => true));
        }

        return $retval;
    }

    /**
     * Delete a value from the datastore.
     *
     * @param string $type  The datatype.
     * @param string $key  The key.
     */
    public function delete($type, $key)
    {
        assert('is_string($type)');
        assert('is_string($key) || is_null($key)');

        if (is_null($key)) {
            // get all values
            $t = array();
            $kv_ix = $this->get_all($type);
            foreach ($kv_ix as $k => $ix) {
                $t[$k] = $this->delete($type, $ix);
            }

            return $this->conn->delete($this->getRequestPath($type));
        }

        Logger::debug('Consul: Delete ' . $type . '/' . $key);
        $this->conn->delete($this->getRequestPath($type, $this->getNestedKey($key)), array('recurse' => true));
        return $this->conn->delete($this->getRequestPath($type, $key));
    }

    /**
     * Clean the key-value table of expired entries.
     */
    public function cleanKVStore()
    {
        // No namespace here
        Logger::debug('store.consul: Cleaning key-value store.');

        $kvarr = $this->get_all("");
        $delc = 0;
        foreach ($kvarr as $t) {
            try {
                list($type, $key) = explode("/", $t, 2);
                $val = $this->conn->get($this->getRequestPath($type, $key), ['raw' => true]);
                $v = json_decode($val->getBody(), true);
                if ($v['expires'] > 0 && $v['expires'] < time()) {
                    $this->delete($type, $key);
                    $delc++;
                }
            } catch (Exception $ex) {
                continue;
            }
        }

        // No namespace here
        Logger::debug("store.consul: Cleanup complete, $delc items removed");
    }

    public function createQueryBuilder()
    {
        throw new \SimpleSAML_Error_Exception("KV based data persistence drivers do not support queries. Please rewrite module to use KV.", 8799);
    }

    /**
     * @return string
     */
    public function getPrefix()
    {
        return $this->mergePath($this->prefix, "v2");
    }

    public function mergePath($root, $key)
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

    private function getRequestPath($root = "", $key = null)
    {

        return $this->mergePath($this->mergePath($this->getPrefix(), $root), $key);
    }

    private function serialize($value)
    {
        return serialize($value);
    }

    private function unserialize($value)
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
            $chunks = $this->get($this->mergePath($this->mergePath($type, $this->getNestedKey($key)), $pl['hash']), null);
            $ccount = intval($payload);

            $payload = "";
            for ($i = 0; $i < $ccount; $i++) {
                $payload .= base64_decode($chunks[$i]);
            }

            if (md5($payload) != $pl['hash']) {
                \SimpleSAML\Logger::error('Consul: decodeValue ' . $this->getRequestPath($type, $key) . ' checksum mismatch (' . $pl['hash'] . '), key deleted ');
                $this->delete($type, $key);
                throw new \SimpleSAML_Error_Exception("Checksum error for stored data", 8798);
            }

            $payload = $this->unserialize($payload);
        }

        return $payload;
    }
}
