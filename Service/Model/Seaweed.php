<?php

namespace Service\Model;

use SC_Seaweed_Exception;
use SC_Seaweed_Storage;
use Service\Model\Seaweed\DeleteQueue;
use Service_Config_Seaweed;
use Sensphere\Analytics\Tracker;
use Service\Model\Analytics\Metric\Profiler\Seaweed as SeaweedProfiler;

class Seaweed extends \Jaumo_Model_Abstract {

	/**
	 * @var Seaweed
	 */
	private static $instance;

	/**
	 * @var Service_Config_Seaweed
	 */
	private $config;

	/**
	 * @var SC_Seaweed_Storage
	 */
	private $storage;

	/**
	 * @var Tracker
	 */
	private $tracker;

	/**
	 * @var SeaweedProfiler
	 */
	protected $profiler;

	protected $isReadAvailable = true;
	protected $isWriteAvailable = true;

	public function __construct(Service_Config_Seaweed $config) {
		parent::__construct();
		$this->config = $config;
		$this->tracker = Tracker::getInstance();
		$this->profiler = SeaweedProfiler::create();
	}

	public static function getInstance() {
		if (!self::$instance) {
			self::$instance = new self(Service_Config_Seaweed::getInstance());
		}

		return self::$instance;
	}

	/**
	 * @param boolean $isReadAvailable
	 */
	public function setIsReadAvailable(bool $isReadAvailable) {
		$this->isReadAvailable = $isReadAvailable;
	}

	public function isReadAvailable() {
		return $this->isReadAvailable;
	}

	/**
	 * @param boolean $isWriteAvailable
	 */
	public function setIsWriteAvailable(bool $isWriteAvailable) {
		$this->isWriteAvailable = $isWriteAvailable;
	}

	public function isWriteAvailable() {
		return $this->isWriteAvailable;
	}

	/**
	 * @param SC_Seaweed_Storage $storage
	 */
	public function setStorage(SC_Seaweed_Storage $storage) {
		$this->storage = $storage;
	}

	/**
	 * @return SC_Seaweed_Storage
	 */
	protected function getStorage() {
		if ($this->storage === null) {
			$this->storage = new SC_Seaweed_Storage($this->config->getTrackers());
		}
		return $this->storage;
	}

	public function invalidateCache($fileId) {
		// get stream context for timeout
		$context = stream_context_create([
			'http' => [
				'timeout' => 1,
			]
		]);

		// invalidate cache
		$url = sprintf($this->config->getInvalidateUrl(), $this->config->getFrontendUrl(), $fileId);
		$errorReporting = error_reporting(E_ALL ^ E_WARNING);
		$result = file_get_contents($url, false, $context);
		error_reporting($errorReporting);

		if (false !== $result) {
			return true;
		}

		return false;
	}

	protected function getVolumeId($fid) {
		list($volumeId, $rest) = explode(',', $fid);
		return $volumeId;
	}

	public function exists($fid, $collection = null) {
		$storage = $this->getStorage();
		$lookup = $storage->lookup($this->getVolumeId($fid), $collection);
		$exception = null;
		foreach ($lookup->getLocations() as $location) {
			try {
				if ($storage->fileExists($location, $fid)) {
					return true;
				}
			}
			catch (\SC_Seaweed_Exception $e) {
				// Try next
				$exception = $e;
			}
		}

		if ($exception !== null) {
			throw $exception;
		}

		return false;
	}

	protected function getReplicationCount($collection) {
		$collections = $this->config->get('collections');
		if (!isset($collections[$collection])) {
			throw new SC_Seaweed_Exception("Undefined collection $collection", SC_Seaweed_Exception::CODE_REQUEST_FAILED);
		}
		return $collections[$collection];
	}

	protected function getTtl($collection) {
		$ttl = $this->config->get('ttl');
		if (!isset($ttl[$collection])) {
			return null;
		}
		return $ttl[$collection];
	}

	public function put($collection, $fileName, $mimeType = 'image/webp') {
		$storage = $this->getStorage();

		$t1 = microtime(true);

		try {
			$replication = $this->getReplicationCount($collection);
			$ttl = $this->getTtl($collection);
			$assign = $storage->assign(1, $collection, $replication, $ttl);
			$fid = $assign->getFid();
			$volumeServer = $assign->getPublicUrl();
			$storage->store($volumeServer, $fid, $fileName, $mimeType, $ttl);

			$t2 = microtime(true);
			$timingMs = ($t2 - $t1) * 1000;
			$this->tracker->track($this->profiler->eventRequestsPut(['collection' => $collection]));
			$this->tracker->track($this->profiler->eventResponsePut($timingMs, ['collection' => $collection]));
		}
		catch (SC_Seaweed_Exception $e) {
			$this->tracker->track($this->profiler->eventException(['collection' => $collection]));
			throw $e;
		}

		return $fid;
	}

	public function get($fid, $collection = null) {
		$t1 = microtime(true);

		$storage = $this->getStorage();
		$locations = $storage->lookup($this->getVolumeId($fid), $collection);
		$exception = null;
		$trackCollection = $collection ?? 'unknown';
		foreach ($locations->getLocations() as $location) {
			try {
				$result = $storage->retrieve($location, $fid);
				$t2 = microtime(true);
				$timingMs = ($t2 - $t1) * 1000;
				$this->tracker->track($this->profiler->eventRequestsGet(['collection' => $trackCollection]));
				$this->tracker->track($this->profiler->eventResponseGet($timingMs, ['collection' => $trackCollection]));
				return $result;
			}
			catch (\SC_Seaweed_Exception $e) {
				// Try next
				$exception = $e;
			}
		}

		$this->tracker->track($this->profiler->eventException(['collection' => $trackCollection]));
		throw $exception;
	}

	public function deleteAssets(\Service_Data_Manager_ImageAsset_Abstract $manager, $assets) {
		foreach ($assets as $size => $image) {
			$class = $manager->getClassForSize($size);
			$this->delete($image['fileId'], $class);
		}
	}

	public function deleteNoQueue($fid, $collection = null) {
		$storage = $this->getStorage();
		$exception = null;
		$replication = $this->getReplicationCount($collection);

		$t1 = microtime(true);

		$trackCollection = $collection ?? 'unknown';
		try {
			$result = $storage->deleteSafe($fid, $replication, $collection);
			$this->invalidateCache($fid);

			$t2 = microtime(true);
			$timingMs = ($t2 - $t1) * 1000;
			$this->tracker->track($this->profiler->eventRequestsDelete(['collection' => $trackCollection]));
			$this->tracker->track($this->profiler->eventResponseDelete($timingMs, ['collection' => $trackCollection]));

			return $result;
		}
		catch (SC_Seaweed_Exception $e) {
			$this->tracker->track($this->profiler->eventException(['collection' => $trackCollection]));
			throw $e;
		}
	}

	public function delete($fid, $collection = null) {
		try {
			$this->deleteNoQueue($fid, $collection);
		}
		catch (\Throwable $e) {
			$replication = $this->getReplicationCount($collection);
			$queue = DeleteQueue::getInstance();
			$queue->enqueue($fid, $collection, $replication);
		}
	}
}