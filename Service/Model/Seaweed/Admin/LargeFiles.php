<?php

namespace Service\Model\Seaweed\Admin;

use SC_Seaweed_Exception;
use Service\Model\Deploy\Execution\DSH;
use Service\Model\Seaweed\Admin;

class LargeFiles extends Admin {

	protected $dryRun = false;

	/**
	 * @param bool $dryRun
	 */
	public function setDryRun(bool $dryRun) {
		$this->dryRun = $dryRun;
	}

	public function findLargeFiles() {
		return $this->findPhysicalVolumeFiles("-size +32G");
	}

	public function repairLargeFiles() {
		$largeFiles = $this->findLargeFiles();
		$volumes = $this->getVolumeDistribution();

		$restores = [];
		foreach ($largeFiles as $volumeId => $places) {
			$collection = $places[0]['collection'];
			$volume = $volumes[$volumeId];
			$replicas = $volume[$collection];
			$largeCount = count($places);
			$replicaCount = count($replicas);
			if ($largeCount >= $replicaCount) {
				$this->logger->warn("Volume $volumeId/$collection cannot be restored from replica");
			}
			else {
				$this->logger->info("Volume $volumeId/$collection has $largeCount too large files and $replicaCount replicas");
				$replicateFrom = [];
				foreach ($replicas as $server) {
					$replicateFrom[$server['server']] = $server;
				}

				foreach ($places as $place) {
					unset($replicateFrom[$place['server']]);
				}

				$replicateInfo = array_pop($replicateFrom);
				$replicateFromServer = $replicateInfo['server'];
				foreach ($places as $place) {
					$targetServer = $place['server'];
					$targetHost = $this->getHostNameFromServerAddress($targetServer);
					$replicateFromHost = $this->getHostNameFromServerAddress($replicateFromServer);
					$humanSize = $this->humanFilesize($place['size']);
					$this->logger->info("Volume $volumeId/$collection will be restored from $replicateFromHost to $targetHost (Size $humanSize)");
					if (!isset($restores[$targetServer])) {
						$restores[$targetServer] = [];
					}
					$replicaPath = $this->findVolumeOnServer($replicateFromServer, $volumeId, $collection);
					$restores[$targetServer][] = [
						'volumeId' => $volumeId,
						'collection' => $collection,
						'path' => $place['path'],
						'replicaServer' => $replicateFromServer,
						'replicaPath' => $replicaPath
					];
				}
			}
		}

		ksort($restores);

		if ($this->dryRun) {
			return;
		}
		foreach ($restores as $targetServer => $files) {
			$dsh = $this->getDshHandlerForVolumeServer($targetServer);
			$targetHost = $this->getHostNameFromServerAddress($targetServer);

			$this->logger->info("Stop volume server on $targetHost");
			$dsh->execute("supervisorctl stop weed_volume");

			foreach ($files as $file) {
				$volumeId = $file['volumeId'];
				$collection = $file['collection'];
				$path = $file['path'];
				$replicaServer = $file['replicaServer'];
				$replicaPath = $file['replicaPath'];
				$replicaHost = $this->getHostNameFromServerAddress($replicaServer);
				$filenames = [
					$this->getBaseName($volumeId, $collection, '.dat'),
					$this->getBaseName($volumeId, $collection, '.idx'),
				];

				foreach ($filenames as $filename) {
					$tmpDir = "$path/tmp";
					$tmpPath = "$tmpDir/$filename";
					$this->logger->info("Copy $replicaHost:$replicaPath/$filename to $targetHost:$tmpPath");
					$dsh->execute("mkdir -p $tmpDir");
					$cmd = "scp $replicaHost:$replicaPath/$filename $tmpPath";
					$dsh->execute($cmd);
				}

				foreach ($filenames as $filename) {
					$tmpDir = "$path/tmp";
					$tmpPath = "$tmpDir/$filename";
					$this->logger->info("Move temporary file to $path/$filename");
					$cmd = "mv $tmpPath $path/$filename";
					$dsh->execute($cmd);
				}
			}

			$this->logger->info("Start volume server on $targetHost");
			$dsh->execute("supervisorctl start weed_volume");
		}
	}

}