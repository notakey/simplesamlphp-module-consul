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
        $consulconfig = \SimpleSAML_Configuration::getConfig('module_consul.php');

        $this->prefix = $consulconfig->getString('kv_prefix', 'sso');
        $url = $consulconfig->getString('kv_url', 'http://localhost:8500');

        $this->sf = new \SensioLabs\Consul\ServiceFactory(['base_uri' => $url]);

        try{
            $this->conn = $this->sf->get('kv');
        }catch(Exception $ex){
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
	public function get($type, $key) {
		assert('is_string($type)');
		assert('is_string($key) || is_null($key)');

        if(is_null($key)){
            // get all values
            $t = array();
            $kv_ix = $this->get_all($type);
            foreach($kv_ix as $k => $ix){
                $t[$k] = $this->get($type, $ix);
            }

            return $t;
        }

        try{
		    $val = $this->conn->get($this->getRequestPath($type, $key), ['raw' => true]);
        }catch(\SensioLabs\Consul\Exception\ClientException $ex){
            return null;
        }

        \SimpleSAML\Logger::debug('Consul: Fetch '.$type.'/'.$key);
        // if($val->getStatusCode() == 404){
        //     return null;
        // }

		return $this->decodeValue($val->getBody());
	}

    private function get_all($type = ""){
        $t = array();

        $path = $this->getRequestPath($type);
        try{
            $val = $this->conn->get($path, ['keys' => true]);
        }catch(\SensioLabs\Consul\Exception\ClientException $ex){
            return $t;
        }

        $kv_ix = json_decode($val->getBody(), true);

        if(!is_array($kv_ix)){
            throw new \SimpleSAML_Error_Exception("Failed index for type $type", 8767);
        }


        // TODO possible OOM exception here, need custom exception and paged loading
        foreach($kv_ix as $ix=>$v){
            $kv_ix[$ix] = str_replace($path, '', $v);
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
	public function set($type, $key, $value, $expire = NULL) {
		assert('is_string($type)');
		assert('is_string($key)');
		assert('is_null($expire) || (is_int($expire) && $expire > 2592000)');
        $encval = $this->encodeValue($key, $value, $expire);

        if(strlen($encval) > (512*1024)){
            throw new \SimpleSAML_Error_Exception("Playload for key $type/$key exceeds limit", 8765);
        }

        \SimpleSAML\Logger::debug('Consul: Store '.$type.'/'.$key.' '.strlen($encval).'B');
		return $this->conn->put($this->getRequestPath($type, $key), $encval);
	}

	/**
	 * Delete a value from the datastore.
	 *
	 * @param string $type  The datatype.
	 * @param string $key  The key.
	 */
	public function delete($type, $key) {
		assert('is_string($type)');
		assert('is_string($key) || is_null($key)');

        if(is_null($key)){
            // get all values
            $t = array();
            $kv_ix = $this->get_all($type);
            foreach($kv_ix as $k => $ix){
                $t[$k] = $this->delete($type, $ix);
            }

            return $this->conn->delete($this->getRequestPath($type));
        }

        \SimpleSAML\Logger::debug('Consul: Delete '.$type.'/'.$key);

		return $this->conn->delete($this->getRequestPath($type, $key));
	}

    /**
     * Clean the key-value table of expired entries.
     */
    public function cleanKVStore()
    {
        Logger::debug('store.consul: Cleaning key-value store.');

        $kvarr = $this->get_all("");
        $delc = 0;
        foreach($kvarr as $t){
            try{
                list($type, $key) = explode("/", $t, 2);
                $val = $this->conn->get($this->getRequestPath($type, $key), ['raw' => true]);
                $v = json_decode($val->getBody(), true);
                if($v['expires'] > 0 && $v['expires'] < time()){
                    $this->delete($type, $key);
                    $delc++;
                }
            }catch(Exception $ex){
                continue;
            }
        }

        Logger::debug("store.consul: Cleanup complete, $delc items removed");

    }

    public function createQueryBuilder(){
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

        if(is_null($key) || $key == ""){
            return $root;
        }

        return "$root/$key";
    }

    private function getRequestPath($root = "", $key = null){

        return $this->mergePath($this->mergePath($this->getPrefix(), $root), $key);
    }

    private function encodeValue($key, $val, $expire = 0){
        $v = ['created_at_str' => date("c"), 'keyname' => $key, 'expires' => $expire, 'payload' => serialize($val)];

        return json_encode($v);
    }

    private function decodeValue($val){
        $pl = json_decode($val, true);

        if($pl['expires'] > 0 && $pl['expires'] < time()){
            return null;
        }

        return unserialize($pl['payload']);
    }

}
