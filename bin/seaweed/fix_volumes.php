#!/usr/bin/env php
<?php

use Service\Model\Deploy\Log\LogInterface;
use Service\Model\Deploy\Transaction;
use Service\Model\Seaweed\Admin\Fix;

$basePath = dirname(dirname(__DIR__));
require_once $basePath . '/app/Bootstrap.php';

Bootstrap::bootEnv(Bootstrap::ENV_PRODUCTION);

$arguments = getopt('hv:l:');
if (isset($arguments['h']) || !isset($arguments['v'])) {
	// show help
	echo "Fix all volumes on a seaweed server\n";
	echo "-v volume server address\n";
	echo "-l log level (" . \Service\Model\Deploy\Log\Helper::description() . ")\n";
	exit;
}

$logLevel = $arguments['l'] ?? LogInterface::LEVEL_INFO;

$transaction = Transaction::createFromArgv($argv, $logLevel, false);
$repair = new Fix(null, $transaction->createLogger());
$repair->fixVolumesOnServer($arguments['v']);
