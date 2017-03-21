#!/usr/bin/env php
<?php

use Service\Model\Deploy\Log\LogInterface;
use Service\Model\Deploy\Transaction;

$basePath = dirname(dirname(__DIR__));
require_once $basePath . '/app/Bootstrap.php';

Bootstrap::bootEnv(Bootstrap::ENV_PRODUCTION);

$arguments = getopt('hv:l:s:dam:w:');
if (isset($arguments['h']) || !isset($arguments['s'])) {
	// show help
	echo "Fix all volumes on a seaweed server\n";
	echo "-v Volume id\n";
	echo "-s Server\n";
	echo "-a Compact all volumes\n";
	echo "-d Dry run\n";
	echo "-m Max compaction age in days\n";
	echo "-w Parallel workers\n";
	echo "-l Log level (" . \Service\Model\Deploy\Log\Helper::description() . ")\n";
	exit;
}

$logLevel = $arguments['l'] ?? LogInterface::LEVEL_INFO;

$transaction = Transaction::createFromArgv($argv, $logLevel, false);
$repair = new \Service\Model\Seaweed\Admin\Compact(null, $transaction->createLogger());
$repair->setDryRun(isset($arguments['d']));
$repair->setWorkers((int)($arguments['w'] ?? 1));
if (isset($arguments['v'])) {
    $repair->compactSingleVolume($arguments['s'], $arguments['v']);
}
else if (isset($arguments['a'])) {
	$days = $arguments['m'] ?? 0;
	$lastCompactionBefore = null;
	if ($days > 0) {
		$lastCompactionBefore = new DateTime("-{$days} days");
	}
	$repair->compactAll($arguments['s'], $lastCompactionBefore);
}
else {
	$repair->compactLargeFiles($arguments['s']);
}
