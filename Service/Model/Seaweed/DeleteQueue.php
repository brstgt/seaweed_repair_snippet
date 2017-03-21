<?php

namespace Service\Model\Seaweed;

use Sensphere\Analytics\Tracker;
use Service\Model\Seaweed;
use Service_Config_Seaweed;
use Service\Model\Analytics\Metric\Profiler\Seaweed as SeaweedProfiler;

class DeleteQueue extends \Jaumo_Model_Abstract {
	/**
	 * @var DeleteQueue
	 */
	private static $instance;

	/**
	 * @var Service_Config_Seaweed
	 */
	private $config;

	/**
	 * @var Tracker
	 */
	private $tracker;

	/**
	 * @var SeaweedProfiler
	 */
	protected $profiler;

	protected $queue;

	public function __construct(Service_Config_Seaweed $config) {
		parent::__construct();
		$this->config = $config;
		$this->tracker = Tracker::getInstance();
		$this->profiler = SeaweedProfiler::create();
		$this->queue = new \SC\Seaweed\DeleteQueue();
	}

	public static function getInstance() {
		if (!self::$instance) {
			self::$instance = new self(Service_Config_Seaweed::getInstance());
		}

		return self::$instance;
	}

	public function enqueue(string $fid, ?string $collection, string $replication) {
		$this->queue->enqueue($fid, $collection, $replication);
		$trackCollection = $collection ?? 'unknown';
		$this->tracker->track($this->profiler->eventDeleteQueueEnqueue(['collection' => $trackCollection]));
	}

	public function process() {
		$trackCollection = $collection ?? 'unknown';
		$seaweed = Seaweed::getInstance();
		$items = $this->queue->pop();
		foreach ($items as $item) {
			try {
				$seaweed->deleteNoQueue($item->getFid(), $item->getCollection());
				$this->queue->dequeue($item->getFid());
				$this->tracker->track($this->profiler->eventDeleteQueueDequeue(['collection' => $trackCollection]));
			}
			catch (\Throwable $e) {
				$this->queue->requeue($item->getFid(), $item->getTryCount(), $e);
				$this->tracker->track($this->profiler->eventDeleteQueueRequeue(['collection' => $trackCollection]));
			}
		}
	}

	public function trackCount() {
		$this->tracker->track($this->profiler->eventDeleteQueueCount($this->queue->count(), [
			'collection' => 'total'
		]));
	}
}
