#!/usr/bin/env php
<?php

use Service\Model\Deploy\Log\LogInterface;

$basePath = dirname(dirname(__DIR__));
require_once $basePath . '/app/Bootstrap.php';

Bootstrap::bootEnv(Bootstrap::ENV_PRODUCTION);

$arguments = getopt('hv:l:');
if (isset($arguments['h']) || !isset($arguments['v'])) {
	// show help
	echo "Fix all volumes on a seaweed server\n";
	echo "-v volume id\n";
	echo "-l log level (" . \Service\Model\Deploy\Log\Helper::description() . ")\n";
	exit;
}

$logLevel = $arguments['l'] ?? LogInterface::LEVEL_INFO;

$repair = new \Service\Model\Seaweed\Admin(null, new \Service\Model\Deploy\Log\Console($logLevel));
$repair->volumeInfo($arguments['v']);
