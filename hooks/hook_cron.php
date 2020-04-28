<?php

function consul_hook_cron(&$croninfo)
{
    $store = \SimpleSAML\Store::getInstance();

    if (!$store instanceof \SimpleSAML\Module\consul\Store\Consul) {
        throw new \SimpleSAML_Error_Exception('OAuth2 module: Only Consul Store is supported');
    }

    $store->cleanKVStore();

    $dbinfo['summary'][] = 'Cleaned Key-Value Store';
}
