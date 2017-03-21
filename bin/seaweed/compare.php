#!/usr/bin/env php
<?php

use Service\Model\Deploy\Log\LogInterface;
use Service\Model\Deploy\Transaction;

$basePath = dirname(dirname(__DIR__));
require_once $basePath . '/app/Bootstrap.php';

Bootstrap::bootEnv(Bootstrap::ENV_PRODUCTION);

$arguments = getopt('hl:');
if (isset($arguments['h'])) {
	// show help
	echo "Compare all volumes\n";
	echo "-l log level (" . \Service\Model\Deploy\Log\Helper::description() . ")\n";
	exit;
}

$logLevel = $arguments['l'] ?? LogInterface::LEVEL_INFO;

$transaction = Transaction::createFromArgv($argv, $logLevel, false);
$repair = new \Service\Model\Seaweed\Admin\Repair(null, $transaction->createLogger());
$repair->compareVolumes(-1);
