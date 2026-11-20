<?php

declare(strict_types=1);

use SimpleSAML\Configuration;
use SimpleSAML\Module\consul\Store\Consul;
use SimpleSAML\Store\StoreFactory;

/**
 * Hook to run a cron job: removes expired entries from the Consul KV store.
 *
 * @param array &$croninfo  Output
 */
function consul_hook_cron(array &$croninfo): void
{
    $config = Configuration::getInstance();
    $store = StoreFactory::getInstance($config->getOptionalString('store.type', 'phpsession'));

    if ($store instanceof Consul) {
        $store->cleanKVStore();
        $croninfo['summary'][] = 'Cleaned Key-Value Store';
    } else {
        $croninfo['summary'][] = 'Store is not of type ' . Consul::class . ', skipped';
    }
}
