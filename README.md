# Consul KV store for SimpleSAMLphp

Stores SimpleSAMLphp sessions and other data in the [Consul](https://www.consul.io/) key-value store.

Version 3.x requires SimpleSAMLphp 2.5 or newer and PHP 8.3 or newer. Use 2.x releases with SimpleSAMLphp 1.x.

## Configuration

`config/config.php`:

```php
'store.type' => 'SimpleSAML\Module\consul\Store\Consul',
'module.enable' => [
    'consul' => true,
    'cron' => true, // expired entries are removed by the cron hook
],
```

`config/module_consul.php`:

```php
$config = [
    'kv_url' => 'http://consul:8500',
    'kv_prefix' => 'sso',
    // split values larger than 400 KB into multiple keys
    'multikey' => false,
];
```

The Consul SDK reads the `CONSUL_HTTP_TOKEN` (ACL token) and `CONSUL_HTTP_ADDR` (overrides `kv_url`) environment variables, when set.
