<?php

namespace Service\Model\Seaweed\Admin;

use DateTime;
use SC_Seaweed_Exception;
use Sensphere_DB_ConnectionPoolManager;
use Service\Model\Seaweed\Admin;

class Repair extends Admin {

	protected $workers = 1;

	/**
	 * @param int $workers
	 */
	public function setWorkers(int $workers) {
		$this->workers = $workers;
	}

	public function repairVolume($volumeId, $collection, $incremental = true, $logPrepend = "") {
		if (empty($logPrepend)) {
			$logPrepend = "($collection/$volumeId) ";
		}
		$newerThan = null;
		if ($incremental) {
			$newerThan = $this->getRepairedAt($volumeId, $collection);
			if ($newerThan === null) {
				$this->logger->info("{$logPrepend}Run first repair");
			}
			else {
				$this->logger->info("{$logPrepend}Run incremental repair, last repair: " . $newerThan->format(DateTime::ISO8601));
			}
		}
		else {
			$this->logger->info("{$logPrepend}Run full repair");
		}

		$this->logger->debug("{$logPrepend}Get servers");
		$repairStart = new \DateTime();
		$servers = $this->getServersForVolumeId($volumeId, $collection);
		$fileIds = [];
		$dateString = $repairStart->format(\DateTime::RFC3339);

		foreach ($servers as $server) {
			$this->logger->info("{$logPrepend}Get files on $server");
			$path = $this->findVolumeOnServer($server, $volumeId, $collection);

			$this->logger->debug("{$logPrepend}List files on $server");
			$allFileIds = $this->listVolumeOnServerPath($server, $path, $volumeId, $collection, $newerThan);
			$ignoreFileIds = $this->listVolumeOnServerPath($server, $path, $volumeId, $collection, $repairStart);
			$ignoreCount = count($ignoreFileIds);
			$this->logger->debug("{$logPrepend}Ignore $ignoreCount files after $dateString on $server");
			foreach ($ignoreFileIds as $fileId => $fileInfo) {
				unset($allFileIds[$fileId]);
			}
			$fileIds[$server] = $allFileIds;
			$count = count($fileIds[$server]);
			$this->logger->debug("{$logPrepend}Found $count files on $server");
		}

		$count = count($servers);
		$missing = [];
		for ($repairIndex = 0; $repairIndex < $count; $repairIndex++) {
			$repairServer = $servers[$repairIndex];
			$missing[$repairServer] = [];
			for ($compareTo = 0; $compareTo < $count; $compareTo++) {
				if ($compareTo == $repairIndex) {
					continue;
				}
				$compareServer = $servers[$compareTo];
				$this->logger->debug("{$logPrepend}Repair $repairServer, compare to $compareServer");
				foreach ($fileIds[$compareServer] as $fileIdOnCompare => $fileInfo) {
					if (!isset($fileIds[$repairServer][$fileIdOnCompare])) {
						$missing[$repairServer][$fileIdOnCompare] = [
							'fileId' => $fileIdOnCompare,
							'fileInfo' => $fileInfo,
							'repairFrom' => $compareServer
						];
					}
				}
			}

			$countMissing = count($missing[$repairServer]);
			if ($countMissing > 0) {
				$this->logger->warn("{$logPrepend}$countMissing files missing on $repairServer");
			}
		}

		$this->syncMissingFiles($missing, $this->getTtl($collection), $logPrepend);
		$this->setRepairedAt($volumeId, $collection, $repairStart);
	}

	public function syncMissingFiles($missing, $ttl = null, $logPrepend = "") {
		foreach ($missing as $repairServer => $missingFileIds) {
			$countMissing = count($missingFileIds);
			if ($countMissing == 0) {
				continue;
			}
			$this->logger->info("{$logPrepend}Repair $countMissing missing files missing on $repairServer");
			$i = 1;
			$storage = $this->getStorage();
			foreach ($missingFileIds as $missingFile) {
				$fileId = $missingFile['fileId'];
				$fileInfo = $missingFile['fileInfo'];
				$repairFrom = $missingFile['repairFrom'];
				$mimeType = $fileInfo['mime'];

				try {
					$lastModified = $storage->getLastModified($repairFrom, $fileId);
					$tmpFile = $storage->retrieve($repairFrom, $fileId);
					$this->logger->command("{$logPrepend}Sync fid $fileId from $repairFrom to $repairServer, mime=$mimeType, ttl=$ttl, last-modified=" . $lastModified->format(DateTime::RFC3339));
					$storage->storeNoReplicate($repairServer, $fileId, $tmpFile->getFileName(), $mimeType, $ttl, $lastModified);
				}
				catch (SC_Seaweed_Exception $e) {
					switch ($e->getCode()) {
						case SC_Seaweed_Exception::CODE_NOT_FOUND:
							$this->logger->debug($logPrepend.$e->getMessage() . " > probably deleted during sync");
							break;
						default:
							$this->logger->exception($e);
					}
				}

				$i++;
				if ($i % 1000 == 0) {
					$this->logger->debug("{$logPrepend}... {$i} repaired");
				}
			}
		}
	}

	/**
	 * @param int $repairThreshold Sync files on volume if file count diverges more than $repairThreshold percent
	 */
	public function compareVolumes($repairThreshold = 10) {
		$logger = $this->logger;
		$this->logger->debug("Start");
		$volumes = $this->getVolumeDistribution();
		$skipCollections = [
			'', 'unittest'
		];

		foreach ($volumes as $volumeId => $volume) {
			if (count($volume) > 1) {
				$desc = [];
				foreach ($volume as $collection => $servers) {
					$serverNames = [];
					foreach ($servers as $server) {
						$serverNames[] = $server['server'];
					}
					$desc[] = $collection . "=" . implode(',', $serverNames);
				}
				$logger->error("Volume $volumeId has more than 1 collection: " . implode(', ', $desc));
			}

			foreach ($volume as $collection => $replications) {
				if (in_array($collection, $skipCollections)) {
					continue;
				}

				// Empty collections are not configured, don't check
				try {
					$expectedReplicationCount = $this->getReplicationCount($collection);
				}
				catch (SC_Seaweed_Exception $e) {
					continue;
				}

				$replicationCount = count($replications);
				if ($expectedReplicationCount != $replicationCount) {
					$servers = [];
					$totalFileCount = 0;
					foreach ($replications as $replication) {
						$fileCount = $replication['volume']->FileCount;
						$totalFileCount += $fileCount;
						$servers[] = $replication['server'] . ", {$fileCount} Files";
					}

					$serverList = implode(', ', $servers);
					$logger->error("Volume $volumeId/$collection expected replication count $expectedReplicationCount, got $replicationCount on $serverList");

					// Delete empty and stale volumes
					if ($totalFileCount == 0) {
						foreach ($replications as $replication) {
							$this->deleteVolume($replication['server'], $volumeId, $collection);
						}
					}
				}

				$storage0 = $replications[0];
				$volume0 = $storage0['volume'];
				$server0 = $storage0['server'];

				if ($repairThreshold >= 0) {
					for ($i = 1; $i < $replicationCount; $i++) {
						$storage = $replications[$i];
						$volume = $storage['volume'];
						$server = $storage['server'];

						if ($volume0->FileCount != $volume->FileCount) {
							$diff = abs($volume0->LiveFileCount - $volume->LiveFileCount);
							$diffPercent = ($diff / $volume0->LiveFileCount) * 100;
							$message = sprintf("File count on $volumeId/$collection differs %0.2f%%." .
								" $server0: {$volume0->FileCount} <> $server: {$volume->FileCount}", $diffPercent);

							switch (true) {
								case $diffPercent > 10:
									$logger->error($message);
									break;
								case $diffPercent > 3:
									$logger->warn($message);
									break;
								case $diffPercent > 1:
									$logger->debug($message);
									break;
							}

							if ($diffPercent > $repairThreshold) {
								$this->logger->info("Start repair on volume $volumeId/$collection");
								$this->repairVolume($volumeId, $collection);
							}
						}
					}
				}
			}
		}
	}

	public function repairVolumes($minDaysSinceLastRepair = 0, $incremental = true) {
		$logger = $this->logger;
		$this->logger->debug("Start");
		$volumes = $this->getVolumeDistribution();
		$skipCollections = [
			'', 'unittest'
		];

		$lastRepairBefore = null;
		if ($minDaysSinceLastRepair > 0) {
			$lastRepairBefore = new DateTime("-$minDaysSinceLastRepair days");
			$logger->info("Repair volumes with last repair before: " . $lastRepairBefore->format(DateTime::ISO8601));
		}

		$queue = [];
		foreach ($volumes as $volumeId => $volume) {
			if (count($volume) > 1) {
				$desc = [];
				foreach ($volume as $collection => $servers) {
					$serverNames = [];
					foreach ($servers as $server) {
						$serverNames[] = $server['server'];
					}
					$desc[] = $collection . "=" . implode(',', $serverNames);
				}
				$logger->error("Volume $volumeId has more than 1 collection: " . implode(', ', $desc));
				continue;
			}

			foreach ($volume as $collection => $replications) {
				if (in_array($collection, $skipCollections)) {
					continue;
				}

				if ($minDaysSinceLastRepair > 0) {
					$lastRepair = $this->getRepairedAt($volumeId, $collection);
					if ($lastRepair != null && $lastRepair > $lastRepairBefore) {
						$logger->debug("Skip volume $volumeId/$collection");
						continue;
					}
				}

				$queue[] = [$volumeId, $collection, $incremental];
			}
		}

		$this->doRepairVolumes($queue);
	}

	protected function doRepairVolumes(array $queue) {
		$processQueue = function(array $queue, $worker = 0) {
			$totalJobs = count($queue);
			$i = 1;
			foreach ($queue as $volume) {
				[$volumeId, $collection, $incremental] = $volume;
				$workerInfo = $worker > 0 ? "(W $worker: $i / $totalJobs, $volumeId/$collection) " : "";
				try {
					$this->repairVolume($volumeId, $collection, $incremental, $workerInfo);
				}
				catch (\Throwable $e) {
					$this->logger->exception($e);
				}
				$i++;
			}
		};

		if ($this->workers <= 1) {
			$processQueue($queue);
		}
		else {
			$queues = [];
			for ($worker = 0; $worker < $this->workers; $worker++) {
				$queues[$worker] = [];
			}

			$worker = 0;
			while (!empty($queue)) {
				$volume = array_shift($queue);
				$queues[$worker][] = $volume;
				$worker++;
				$worker = $worker % $this->workers;
			}

			$jobs = [];
			foreach ($queues as $worker => $queue) {
				shuffle($queue);
				$jobs[] = function () use ($worker, $processQueue, $queue) {
					$processQueue($queue, $worker + 1);
				};
			}

			\Jaumo_Application::closeAllConnections();
			$fork = new \Sensphere_Fork();
			$fork->executeParallel($jobs);
		}
	}

	protected function getDb() {
		return Sensphere_DB_ConnectionPoolManager::getInstance()->getConnectionPool('log')->getConnectionSync();
	}

	public function getRepairedAt(int $volumeId, string $collection) {
		$db = $this->getDb();
		$r = new \Sensphere_DB_MySql_Result($db->query("SELECT repaired_at 
			FROM seaweed_repair_status 
			WHERE volume_id = $volumeId AND collection = " . $db->quote($collection)));
		$repairedAt = $r->getOne();
		if (empty($repairedAt)) {
			return null;
		}

		return new DateTime($repairedAt);
	}

	public function setRepairedAt(int $volumeId, string $collection, DateTime $repairedAt) {
		$db = $this->getDb();
		$sql = "INSERT INTO seaweed_repair_status SET 
			volume_id = $volumeId,
			collection = " . $db->quote($collection) . ",
			repaired_at = '" . $repairedAt->format(DateTime::ISO8601) . "'
		ON DUPLICATE KEY UPDATE 
			repaired_at = '" . $repairedAt->format(DateTime::ISO8601) . "'
			";
		$db->query($sql);
	}

}