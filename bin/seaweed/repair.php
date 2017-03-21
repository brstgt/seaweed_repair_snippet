#!/usr/bin/env php
<?php

use Service\Model\Deploy\Log\LogInterface;
use Service\Model\Deploy\Transaction;

$basePath = dirname(dirname(__DIR__));
require_once $basePath . '/app/Bootstrap.php';

Bootstrap::bootEnv(Bootstrap::ENV_PRODUCTION);

$arguments = getopt('hl:d:f:w:v:c:');
if (isset($arguments['h'])) {
	echo "Repair all volumes\n";
	echo "-d Minimum days since last repair\n";
	echo "-f Run full repair\n";
	echo "-w Parallel workers\n";
	echo "-v volumeId\n";
	echo "-c collection\n";
	echo "-l Log level (" . \Service\Model\Deploy\Log\Helper::description() . ")\n";
	exit;
}

$logLevel = $arguments['l'] ?? LogInterface::LEVEL_INFO;

$transaction = Transaction::createFromArgv($argv, $logLevel, false);
$repair = new \Service\Model\Seaweed\Admin\Repair(null, $transaction->createLogger(1024));
$repair->setWorkers((int)($arguments['w'] ?? 1));
if (isset($arguments['v'])) {
    $repair->repairVolume($arguments['v'], $arguments['c'] ?? null, !isset($arguments['f']));
}
else {
    $repair->repairVolumes((int)($arguments['d'] ?? 0), !isset($arguments['f']));
}
