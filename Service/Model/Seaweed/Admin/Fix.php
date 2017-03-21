<?php

namespace Service\Model\Seaweed\Admin;

use SC_Seaweed_Exception;
use Service\Model\Seaweed\Admin;

class Fix extends Admin {

	protected function fixVolumeOnServer($volumeServerAddress, $path, $volumeId, $collection) {
		$dsh = $this->getDshHandlerForVolumeServer($volumeServerAddress);
		$cmd = "weed fix -dir $path -volumeId $volumeId -collection $collection";
		$result = $dsh->execute($cmd, true);
		if ($result != 0) {
			$error = implode(",", $dsh->getLastResponse());
			throw new SC_Seaweed_Exception("Could not fix volume $collection/$volumeId from $volumeServerAddress: $error");
		}
	}

	public function fixVolumesOnServer($volumeServer) {
		$this->logger->info("Fix volumes on server $volumeServer");
		$volumesOnServer = $this->getVolumesOnServer($volumeServer);
		foreach ($volumesOnServer as $volume) {
			$volumeId = $volume->Id;
			$collection = $volume->Collection;
			if (!empty($collection)) {
				$path = $this->findVolumeOnServer($volumeServer, $volumeId, $collection);
				$this->logger->info("Fix volume $volumeServer:$path:$volumeId:$collection");
				try {
					$this->fixVolumeOnServer($volumeServer, $path, $volumeId, $collection);
				}
				catch (\Throwable $e) {
					$this->logger->exception($e);
				}
			}
		}
	}

	public function fixVolumes() {
		$volumeServers = $this->getVolumeServersOnMaster($this->getMasterServerLeader());
		foreach ($volumeServers as $volumeServer) {
			$this->fixVolumesOnServer($volumeServer);
		}
	}

}