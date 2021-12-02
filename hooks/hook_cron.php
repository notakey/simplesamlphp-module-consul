<?php

function consul_hook_cron(&$croninfo)
{
    $store = \SimpleSAML\Store::getInstance();

    if ($store instanceof \SimpleSAML\Module\consul\Store\Consul) {
        $store->cleanKVStore();
        $croninfo['summary'][] = 'Cleaned Key-Value Store';
    } else {
        // throw new \SimpleSAML\Error\Exception('OAuth2 module: Only Consul Store is supported');
        $croninfo['summary'][] = 'Store is not of type \SimpleSAML\Module\consul\Store\Consul, skipped';
    }
}
