<?php

namespace Service\Model\Seaweed\Admin;

use DateTime;
use Sensphere_DB_ConnectionPoolManager;
use Service\Model\Seaweed\Admin;

class Compact extends Admin {

	const MAX_SIZE = 32 * 1024 * 1024 * 1024;

	protected $dryRun = false;

	protected $workers = 1;

	protected $createBackup = false;

	/**
	 * @param bool $dryRun
	 */
	public function setDryRun(bool $dryRun) {
		$this->dryRun = $dryRun;
	}

	/**
	 * @param int $workers
	 */
	public function setWorkers(int $workers) {
		$this->workers = $workers;
	}

	protected function getVolumeFiles($volumeServer) {
		return $this->findPhysicalVolumeFiles("", $volumeServer);
	}

	protected function getVolumesWithFiles($volumeServer) {
		$files = $this->getVolumeFiles($volumeServer);
		$result = [];
		foreach ($files as $volumeId => $file) {
			$result[] = $file[0];
		}
		return $result;
	}

	public function compactSingleVolume($volumeServer, $volumeIdsString) {
		$this->logger->info("Compacting volume $volumeIdsString on $volumeServer");
		$volumeIds = [];
		foreach (explode(',', $volumeIdsString) as $volumeId) {
			$volumeIds[] = (int)$volumeId;
		}
		$volumes = $this->getVolumesWithFiles($volumeServer);
		$result = [];
		foreach ($volumes as $volume) {
			if (in_array($volume['volumeId'], $volumeIds)) {
				$result[] = $volume;
			}
		}

		$this->compactList($volumeServer, $result);
	}

	public function compactLargeFiles($volumeServer) {
		$this->logger->info("Compacting too large files on $volumeServer");
		$volumes = $this->getVolumesWithFiles($volumeServer);
		$result = [];
		foreach ($volumes as $volume) {
			if ($volume['size'] > self::MAX_SIZE) {
				$result[] = $volume;
			}
		}

		$this->compactList($volumeServer, $result);
	}

	public function compactAll($volumeServer, ?DateTime $lastCompactionBefore = null) {
		if ($lastCompactionBefore !== null) {
			$this->logger->info("Compacting all files on $volumeServer with compaction before ".$lastCompactionBefore->format(\DateTime::ISO8601));
		}
		else {
			$this->logger->info("Compacting all files on $volumeServer");
		}
		$result = $volumes = $this->getVolumesWithFiles($volumeServer);

		if ($lastCompactionBefore !== null) {
			$result = [];
			foreach ($volumes as $volume) {
				$volumeId = $volume['volumeId'];
				$collection = $volume['collection'] ?? "";
				$lastCompaction = $this->getCompactedAt($volumeServer, $volumeId, $collection);
				if ($lastCompaction === null || $lastCompaction < $lastCompactionBefore) {
					$result[] = $volume;
				}
				else {
					$this->logger->info("Skip $volumeId/$collection, last compaction was ".$lastCompaction->format(\DateTime::ISO8601));
				}
			}
		}

		$this->compactList($volumeServer, $result);
	}

	protected function compactList($volumeServer, array $volumes) {
		if (empty($volumes)) {
			$this->logger->info("Nothing to repair");
			return;
		}
		$volumeServerAddress = $this->getServerAddressFromHostname($volumeServer);

		/**
		 * @param $volumes
		 * @param $volumeServerAddress
		 * @param $volumeServer
		 * @param null $worker
		 */
		$processVolumes = function($volumes, $volumeServerAddress, $volumeServer, $worker = null) {
			$storage = $this->getStorage();
			$workerInfo = $worker !== null ? "Q $worker: " : "";
			$dsh = $this->getDshHandlerForVolumeServer($volumeServerAddress);
			$total = count($volumes);
			$totalSize = $processedSize = 0;
			foreach ($volumes as $volume) {
				$totalSize += $volume['size'];
			}

			$i = 0;
			while (true) {
				$locked = [];
				$percent = 0;
				foreach ($volumes as $volume) {
					$volumeId = (int)$volume['volumeId'];
					$collection = $volume['collection'] ?? "";
					$taskInfo = sprintf("($workerInfo$i/$total, %3.2f%%, $volumeId/$collection) ", $percent);
					$path = $volume['path'];
					$byteSize = $volume['size'];

					if (!$this->lock($volumeId, $collection, $volumeServer)) {
						$this->logger->info("{$taskInfo}Could not lock - defer");
						$locked[] = $volume;
						continue;
					}

					$i++;

					try {
						if (!$this->dryRun) {
							$this->logger->debug("{$taskInfo}Unmount");
							$storage->volumeUnmount($volumeServerAddress, $volumeId);
						}

						$size = $this->humanFilesize($byteSize);
						$this->logger->info("{$taskInfo}Start compaction in $path (Size: $size)");

						if (!$this->dryRun) {
							$collParam = empty($collection) ? "" : " -collection $collection";
							$dsh->execute("weed compact -dir $path -volumeId $volumeId$collParam 2>&1");
							$response = $dsh->getLastResponse();
							foreach ($response as $line) {
								if (preg_match("/out of memory/i", $line) > 0) {
									$this->logger->error("{$taskInfo}Out of memory skip");
									continue 2;
								}

								if (preg_match("/no space left/i", $line) > 0) {
									$this->logger->error("{$taskInfo}Device full");
									throw new \RuntimeException("{$taskInfo}Device full");
								}
							}
							$compactedDataFile = $this->getVolumePath($path, $volumeId, $collection, 'cpd');
							$dataFile = $this->getVolumePath($path, $volumeId, $collection, 'dat');
							$compactedIndexFile = $this->getVolumePath($path, $volumeId, $collection, 'cpx');
							$indexFile = $this->getVolumePath($path, $volumeId, $collection, 'idx');
							$this->logger->debug("{$taskInfo}Move compacted data files");
							if ($this->createBackup) {
								$dsh->execute("mv $dataFile $dataFile.bak");
								$dsh->execute("mv $indexFile $indexFile.bak");
							}
							$dsh->execute("mv $compactedDataFile $dataFile");
							$dsh->execute("mv $compactedIndexFile $indexFile");

							$this->setCompactedAt($volumeServer, $volumeId, $collection, new DateTime());
							$storage->volumeMount($volumeServerAddress, $volumeId);
							$this->logger->debug("{$taskInfo}Mount");
							$this->unlock($volumeId, $collection);
						}
					}
					catch (\RuntimeException $e) {
						return;
					}
					finally {
						if (!$this->dryRun) {
							$this->unlock($volumeId, $collection);
						}
					}

					$processedSize += $byteSize;

					$percent = $processedSize / $totalSize * 100;
					$this->logger->info(sprintf("{$taskInfo}Finished $volumeId/$collection, %0.2f%% completed", $percent));
				}

				if (empty($locked)) {
					break;
				}

				$volumes = $locked;
			}
		};

		if ($this->workers <= 1) {
			$processVolumes($volumes, $volumeServerAddress, $volumeServer);
		}
		else {
			$queues = [];
			$sizes = [];
			for ($worker = 0; $worker < $this->workers; $worker++) {
				$queues[$worker] = [];
				$sizes[$worker] = 0;
			}

			$getWorker = function($sizes) {
				asort($sizes);
				foreach ($sizes as $worker => $size) {
					return $worker;
				}
			};
			while (!empty($volumes)) {
				$volume = array_shift($volumes);
				$nextWorker = $getWorker($sizes);
				$queues[$nextWorker][] = $volume;
				$sizes[$nextWorker] += $volume['size'];
			}

			$jobs = [];
			foreach ($queues as $worker => $queue) {
				$jobs[] = function () use ($worker, $processVolumes, $queue, $volumeServerAddress, $volumeServer) {
					$processVolumes($queue, $volumeServerAddress, $volumeServer, $worker + 1);
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

	public function getCompactedAt(string $host, int $volumeId, string $collection) {
		$db = $this->getDb();
		$r = new \Sensphere_DB_MySql_Result($db->query("SELECT compacted_at 
			FROM seaweed_compaction_status 
			WHERE host = ". $db->quote($host)." AND volume_id = $volumeId AND collection = " . $db->quote($collection)));
		$repairedAt = $r->getOne();
		if (empty($repairedAt)) {
			return null;
		}

		return new DateTime($repairedAt);
	}

	public function setCompactedAt(string $host, int $volumeId, string $collection, DateTime $compactedAt) {
		$db = $this->getDb();
		$sql = "INSERT INTO seaweed_compaction_status SET 
			host = ". $db->quote($host).",
			volume_id = $volumeId,
			collection = " . $db->quote($collection) . ",
			compacted_at = '" . $compactedAt->format(DateTime::ISO8601) . "'
		ON DUPLICATE KEY UPDATE 
			compacted_at = '" . $compactedAt->format(DateTime::ISO8601) . "'
			";
		$db->query($sql);
	}

	public function lock(int $volumeId, string $collection, string $host): bool {
		try {
			$db = $this->getDb();
			$sql = "INSERT INTO seaweed_compaction_lock SET 
				volume_id = $volumeId,
				collection = " . $db->quote($collection) . ",
				host = " . $db->quote($host) . ",
				locked_at = NOW()";
			$db->query($sql);
			return true;
		}
		catch (\Sensphere_DB_DuplicateException $e) {
			return false;
		}
	}

	public function unlock(int $volumeId, string $collection) {
		$db = $this->getDb();
		$sql = "DELETE FROM seaweed_compaction_lock WHERE 
			volume_id = $volumeId AND
			collection = " . $db->quote($collection);
		$db->query($sql);
	}
}