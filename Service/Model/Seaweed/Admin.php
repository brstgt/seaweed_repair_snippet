<?php

namespace Service\Model\Seaweed;

use SC_Seaweed_Exception;
use SC_Seaweed_Storage;
use Service\Model\Deploy\Execution\DSH;
use Service\Model\Deploy\Log\Console;
use Service\Model\Deploy\Log\LogInterface;
use Service_Config_Seaweed;

class Admin extends \Jaumo_Model_Abstract {

	/**
	 * @var Service_Config_Seaweed
	 */
	private $config;

	/**
	 * @var SC_Seaweed_Storage
	 */
	private $storage;

	/**
	 * @var LogInterface
	 */
	protected $logger;

	/**
	 * @var DSH[]
	 */
	protected $dshHandlers;

	public function __construct(Service_Config_Seaweed $config = null, LogInterface $logger) {
		parent::__construct();
		$this->config = $config ?? new Service_Config_Seaweed();
		$this->logger = $logger;
		$ini = new \SC_IniSet(\SC_IniSet::MEMORY_LIMIT);
		$ini->increaseTo("8G");
	}

	/**
	 * @param LogInterface $logger
	 */
	public function setLogger(LogInterface $logger) {
		$this->logger = $logger;
	}

	/**
	 * @return SC_Seaweed_Storage
	 */
	public function getStorage() {
		if ($this->storage === null) {
			$this->storage = new SC_Seaweed_Storage($this->config->getTrackers());
		}
		return $this->storage;
	}

	protected function getStorageForMaster($masterAddress) {
		return new SC_Seaweed_Storage([$masterAddress]);
	}

	protected function getReplicationCount($collection) {
		$collections = $this->config->get('collections');
		if (!isset($collections[$collection])) {
			throw new SC_Seaweed_Exception("Undefined collection $collection", SC_Seaweed_Exception::CODE_REQUEST_FAILED);
		}
		return $this->getStorage()->getReplicationCount($collections[$collection]);
	}

	protected function getTtl($collection) {
		$ttl = $this->config->get('ttl');
		if (!isset($ttl[$collection])) {
			return null;
		}
		return $ttl[$collection];
	}

	public function getServersForVolumeId($volumeId, $collection) {
		$volumes = $this->getVolumeDistribution();
		$servers = [];
		$replications = $volumes[$volumeId][$collection];
		foreach ($replications as $replication) {
			$servers[] = $replication['server'];
		}

		return $servers;
	}

	public function getVolumeDistribution() {
		$volumeServers = $this->getVolumeServersOnMaster($this->getMasterServerLeader());
		$volumes = [];
		foreach ($volumeServers as $volumeServer) {
			$volumesOnServer = $this->getVolumesOnServer($volumeServer);
			foreach ($volumesOnServer as $volume) {
				$volumeId = $volume->Id;
				$collection = $volume->Collection;
				if (!isset($volumes[$volumeId])) {
					$volumes[$volumeId] = [];
				}
				if (!isset($volumes[$volumeId][$collection])) {
					$volumes[$volumeId][$collection] = [];
				}
				$volumes[$volumeId][$collection][] = [
					'server' => $volumeServer,
					'volume' => $volume
				];
			}
		}
		ksort($volumes);
		foreach ($volumes as &$volume) {
			ksort($volume);
		}

		return $volumes;
	}

	protected function getMasterServers() {
		$s = $this->getStorage()->clusterStatus();
		$masters[$s->Leader] = $s->Leader;
		foreach ($s->Peers as $peer) {
			$masters[$peer] = $peer;
		}

		return $masters;
	}

	protected function getMasterServerLeader() {
		$s = $this->getStorage()->clusterStatus();
		return $s->Leader;
	}

	protected function getVolumeServersPerMaster() {
		$masters = $this->getMasterServers();
		$all = [];
		foreach ($masters as $masterAddress) {
			$volumeServerAddresses = $this->getVolumeServersOnMaster($masterAddress);
			$all[$masterAddress] = $volumeServerAddresses;
		}
		return $all;
	}

	protected function getVolumeServersOnMaster($masterAddress) {
		$this->logger->debug("Get volume servers on $masterAddress");
		$storage = $this->getStorageForMaster($masterAddress);
		$s = $storage->status();
		$volumeServerAddresses = [];
		foreach ($s->Topology->DataCenters as $dataCenter) {
			foreach ($dataCenter->Racks as $rack) {
				foreach ($rack->DataNodes as $node) {
					$volumeServerAddresses[] = 'http://' . $node->Url;
				}

			}
		}

		asort($volumeServerAddresses);
		$volumeServerAddresses = array_values($volumeServerAddresses);

		return $volumeServerAddresses;
	}

	protected function getVolumesOnServer($volumeServerAddress) {
		$this->logger->debug("Get volumes on $volumeServerAddress");

		$result = [];
		foreach ($this->getStorage()->volumeServerStatus($volumeServerAddress)->Volumes as $volume) {
			$volume->LiveSize = $volume->Size - $volume->DeletedByteCount;
			$volume->LiveFileCount = $volume->FileCount - $volume->DeleteCount;
			$volume->GarbageRatio = $volume->FileCount == 0 ? 0 : $volume->DeleteCount / $volume->FileCount;

			$result[] = $volume;
		}
		return $result;
	}

	protected function getDisksOnServer($volumeServerAddress) {
		return $this->getStorage()->volumeServerStatsDisk($volumeServerAddress)->DiskStatuses;
	}

	protected function findVolumeOnServer($volumeServerAddress, $volumeId, $collection) {
		$this->logger->debug("Find $collection/$volumeId on $volumeServerAddress");
		$disks = $this->getDisksOnServer($volumeServerAddress);
		$dsh = $this->getDshHandlerForVolumeServer($volumeServerAddress);
		foreach ($disks as $disk) {
			$path = $disk->Dir;
			$file = "{$collection}_$volumeId.dat";
			$result = $dsh->execute("ls $path/$file", true);
			if ($result == 0) {
				return $path;
			}
		}

		throw new SC_Seaweed_Exception("Volume {$collection}/$volumeId not found on $volumeServerAddress");
	}

	protected function parseExportLine($line) {
		// key=627,115c4795fccfd027 Name=moU9VSqC.jpg Size=59824 gzip=false mime=image/webp
		$parts = explode(' ', trim($line));
		$result = [];
		foreach ($parts as $part) {
			if (strpos($part, "=") !== false) {
				list($key, $value) = explode("=", $part, 2);
				$result[$key] = $value;
			}
		}
		return $result;
	}

	protected function listVolumeOnServerPath($volumeServerAddress, $path, $volumeId, $collection, ?\DateTime $newerThan = null) {
		$dsh = $this->getDshHandlerForVolumeServer($volumeServerAddress);
		$cmd = "weed export -dir $path -volumeId $volumeId -collection $collection";
		if ($newerThan !== null) {
			$cmd .= " -newer '" . $newerThan->format('Y-m-d\TH:i:s') . "'";
		}
		$result = $dsh->execute($cmd, true);
		if ($result == 0) {
			$fileIds = [];
			$listRaw = $dsh->getLastResponse();
			foreach ($listRaw as $line) {
				$file = $this->parseExportLine($line);
				if (!isset($file['key'])) {
					throw new SC_Seaweed_Exception("Could parse export volume $collection/$volumeId from $volumeServerAddress, line: $line");
				}
				$fileId = $file['key'];
				$fileIds[$fileId] = $file;
			}

			return $fileIds;
		}

		throw new SC_Seaweed_Exception("Could not export volume $collection/$volumeId from $volumeServerAddress");
	}

	protected function listVolumeOnServer($volumeServerAddress, $volumeId, $collection, ?\DateTime $newerThan = null) {
		$path = $this->findVolumeOnServer($volumeServerAddress, $volumeId, $collection);
		return $this->listVolumeOnServerPath($volumeServerAddress, $path, $volumeId, $collection, $newerThan);
	}

	protected function getHostNameFromServerAddress($volumeServerAddress) {
		$url = parse_url($volumeServerAddress);
		return $url['host'];
	}

	protected function getServerAddressFromHostname($volumeServer) {
		return "http://$volumeServer:8080";
	}

	protected function getDshHandlerForVolumeServer($volumeServerAddress) {
		if (!isset($this->dshHandlers[$volumeServerAddress])) {
			$host = $this->getHostNameFromServerAddress($volumeServerAddress);
			$dsh = new DSH($this->logger);
			$dsh->setNode($host);
			$dsh->setSudoUser('root');
			$this->dshHandlers[$volumeServerAddress] = $dsh;
		}
		return $this->dshHandlers[$volumeServerAddress];
	}

	public function deleteVolume($volumeServerAddress, $volumeId, $collection) {
		$this->logger->warn("Delete $collection/$volumeId on $volumeServerAddress");
		try {
			$path = $this->findVolumeOnServer($volumeServerAddress, $volumeId, $collection);
			$dsh = $this->getDshHandlerForVolumeServer($volumeServerAddress);
			$dsh->execute("rm ".$this->getVolumePath($path, $volumeId, $collection, 'dat'));
			$dsh->execute("rm ".$this->getVolumePath($path, $volumeId, $collection, 'idx'));
		}
		catch (SC_Seaweed_Exception $e) {
			$this->logger->error($e->getMessage());
		}
	}

	public function volumeInfo($volumeId) {
		$volumes = $this->getVolumeDistribution();
		$volume = $volumes[$volumeId];
		foreach ($volume as $collection => $servers) {
			foreach ($servers as $server) {
				$vol = $server['volume'];
				$this->logger->info(sprintf("Found on {$server['server']} collection={$vol->Collection} garbage=%0.0f%% ".
					"liveSize={$vol->LiveSize} liveFiles={$vol->LiveFileCount}", $vol->GarbageRatio * 100));
			}
		}
	}

	protected function humanFilesize($size, $precision = 2) {
		$units = array('B', 'kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB');
		$step = 1024;
		$i = 0;
		while (($size / $step) > 0.9) {
			$size = $size / $step;
			$i++;
		}
		return round($size, $precision) . $units[$i];
	}

	public function findPhysicalVolumeFiles($options = "", $node = null) {
		$dsh = new DSH($this->logger);
		if ($node !== null) {
			$dsh->setNode($node);
		}
		else {
			$dsh->setGroup('seaweed');
		}
		$dsh->setSudoUser('root');
		$cmd = "find /weedfs/ -name *.dat -type f $options -exec ls -la {} \\;";
		$result = $dsh->execute($cmd, true);
		if ($result == 0) {
			$listRaw = $dsh->getLastResponse();
			$list = [];
			foreach ($listRaw as $line) {
				$file = preg_split('/[ ]+/', $line);
				$server = substr($file[0], 0, -1);
				$path = $file[9];
				$size = $file[5];
				$filename = basename($path);
				[$name, $suffix] = explode('.', $filename);
				if (strpos($name, '_')) {
					[$collection, $volumeId] = explode('_', $name);
				}
				else {
					$collection = null;
					$volumeId = $name;
				}

				if (!isset($list[$volumeId])) {
					$list[$volumeId] = [];
				}
				$list[$volumeId][] = [
					'server' => 'http://' . gethostbyname($server) . ':8080',
					'path' => dirname($path),
					'size' => $size,
					'volumeId' => $volumeId,
					'collection' => $collection,
				];
			}
			return $list;
		}

		throw new SC_Seaweed_Exception("Could find files: " . implode(";", $dsh->getLastResponse()));
	}

	protected function getVolumePath($path, $volumeId, $collection = null, $suffix = 'dat') {
		return $path.'/'.$this->getBaseName($volumeId, $collection, $suffix);
	}

	protected function getBaseName($volumeId, $collection = null, $suffix = 'dat') {
		if (empty($collection)) {
			return "$volumeId.$suffix";
		}

		return "{$collection}_{$volumeId}.$suffix";
	}
}