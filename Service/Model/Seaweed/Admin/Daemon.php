<?php

namespace Service\Model\Seaweed\Admin;

use SC_Seaweed_Exception;
use Service\Model\Seaweed\Admin;

class Daemon extends Admin {

	public function isVolumeServerAlive($volumeServerAddress) {
		try {
			$storage = $this->getStorage()->volumeServerStatus($volumeServerAddress);
			return !empty($storage->Volumes) && is_array($storage->Volumes);
		}
		catch (SC_Seaweed_Exception|\SC_Http_Request_ConnectException $e) {
			return false;
		}
	}

	public function restartVolumeServersRolling() {
		$volumeServers = $this->getVolumeServersOnMaster($this->getMasterServerLeader());
		foreach ($volumeServers as $volumeServer) {
			$this->restartVolumeServer($volumeServer);
		}
	}

	public function restartVolumeServer($volumeServerAddress) {
		if (!$this->isVolumeServerAlive($volumeServerAddress)) {
			throw new SC_Seaweed_Exception("$volumeServerAddress not alive, cannot reboot");
		}
		$this->logger->info("Reboot volume server on $volumeServerAddress");
		$dsh = $this->getDshHandlerForVolumeServer($volumeServerAddress);
		$dsh->execute("supervisorctl restart weed_volume");
		$start = time();
		while (!$this->isVolumeServerAlive($volumeServerAddress)) {
			$elapsed = time() - $start;
			$this->logger->debug("Wait for $volumeServerAddress to come back online. {$elapsed}s");
			sleep(5);
		}
	}

	public function stopVolumeServer($volumeServerAddress) {
		if (!$this->isVolumeServerAlive($volumeServerAddress)) {
			throw new SC_Seaweed_Exception("$volumeServerAddress not alive, cannot reboot");
		}
		$this->logger->info("Stop volume server on $volumeServerAddress");
		$dsh = $this->getDshHandlerForVolumeServer($volumeServerAddress);
		$dsh->execute("supervisorctl stop weed_volume");
	}

	public function startVolumeServer($volumeServerAddress) {
		$this->logger->info("Start volume server on $volumeServerAddress");
		$dsh = $this->getDshHandlerForVolumeServer($volumeServerAddress);
		$dsh->execute("supervisorctl start weed_volume");
		$start = time();
		while (!$this->isVolumeServerAlive($volumeServerAddress)) {
			$elapsed = time() - $start;
			$this->logger->debug("Wait for $volumeServerAddress to come back online. {$elapsed}s");
			sleep(5);
		}
	}
}