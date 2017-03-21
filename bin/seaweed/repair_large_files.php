#!/usr/bin/env php
<?php

use Service\Model\Deploy\Log\LogInterface;
use Service\Model\Deploy\Transaction;

$basePath = dirname(dirname(__DIR__));
require_once $basePath . '/app/Bootstrap.php';

Bootstrap::boot(Bootstrap::ENV_PRODUCTION);

$arguments = getopt('hl:');
if (isset($arguments['h'])) {
	// show help
	echo "Fix all volumes on a seaweed server\n";
    echo "-l log level (".\Service\Model\Deploy\Log\Helper::description().")\n";
	exit;
}

$logLevel = $arguments['l'] ?? LogInterface::LEVEL_INFO;

$transaction = Transaction::createFromArgv($argv, $logLevel, false);
$repair = new \Service\Model\Seaweed\Admin\LargeFiles(null, $transaction->createLogger());
$repair->setDryRun(true);
$repair->repairLargeFiles();
