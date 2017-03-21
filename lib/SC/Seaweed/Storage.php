<?php

class SC_Seaweed_Storage {

	/**
	 * @var string[]
	 */
	protected $storageAddresses = [];

	/**
	 * @var SC_Cache_Interface
	 */
	protected $cache;

	/**
	 * SC_Seaweed_Php constructor.
	 * @param string[] $storageAddresses
	 */
	public function __construct($storageAddresses) {
		$this->storageAddresses = $storageAddresses;
		$this->cache = SC_Cache_Factory::getInstance()->getCache();
	}

	/**
	 *
	 * Get a fid and a volume server url
	 *
	 * for replication options see:
	 * http://code.google.com/p/weed-fs/#Rack-Aware_and_Data_Center-Aware_Replication
	 *
	 * @param int $count
	 * @param string|null $collection
	 * @param string|null $replication
	 * @param string|null $ttl
	 * @return \SC\Seaweed\Result\Assign
	 */
	public function assign(int $count = 1, ?string $collection = null, ?string $replication = null, ?string $ttl = null) {
		$url = '/dir/assign';
		$url .= '?count=' . intval($count);

		if ($replication !== null) {
			$url .= '&replication=' . $replication;
		}

		if ($collection !== null) {
			$url .= '&collection=' . rawurlencode($collection);
		}

		if ($ttl !== null) {
			$url .= '&ttl=' . rawurlencode($ttl);
		}

		// {"count":1,"fid":"3,01637037d6","url":"127.0.0.1:8080","publicUrl":"localhost:8080"}
		return new \SC\Seaweed\Result\Assign($this->doMasterGetRequest($url));
	}

	protected function doMasterGetRequest($url) {
		$available = $this->storageAddresses;
		$exception = null;
		while (!empty($available)) {
			$storageUrl = array_pop($available);
			try {
				$absoluteUrl = "http://$storageUrl$url";
				$response = SC_Http_Request::create($absoluteUrl)->send();
				if ($response->getCode() !== 200) {
					throw new SC_Seaweed_Exception($response->getJson()->error, $response->getCode());
				}
				return $response->getJson();
			}
			catch (SC_Http_Request_ConnectException $e) {
				// Continue
			}
		}

		throw new SC_Seaweed_Exception("Could not connect to any master", SC_Seaweed_Exception::CODE_CONNECT_ERROR);
	}

	/**
	 * @param $url
	 * @return mixed
	 * @throws SC_Seaweed_Exception
	 */
	protected function doGetRequest($url) {
		$response = SC_Http_Request::create($url)->send();
		if ($response->getCode() > 299) {
			throw new SC_Seaweed_Exception("Request failed with code {$response->getCode()}: {$response->getBody()}", SC_Seaweed_Exception::CODE_REQUEST_FAILED);
		}
		return $response->getJson();
	}

	protected function getVolumeId($fid) {
		list($volumeId, $rest) = explode(',', $fid);
		return $volumeId;
	}

	public function getReplicationCount(string $replication) {
		$l = strlen($replication);
		$count = 0;

		for ($x = 0; $x < $l; $x++) {
			$count += (int)$replication[$x];
		}
		return $count + 1;
	}

	/**
	 * Deletes a file only if required replication count can be matched.
	 *
	 * @param string $fid
	 * @param string $replication
	 * @param string|null $collection
	 * @return mixed
	 * @throws SC_Seaweed_Exception
	 */
	public function deleteSafe(string $fid, string $replication, ?string $collection = null) {
		$replicationCount = $this->getReplicationCount($replication);
		$volumeId =$this->getVolumeId($fid);
		$lookup = $this->getLookupResult($this->rawLookup($volumeId), $volumeId, null);
		$count = count($lookup->getLocations());
		if ($count != $replicationCount) {
			throw new SC_Seaweed_Exception("Required replication count not met for $fid/$collection, expected $replicationCount, got $count");
		}

		return $this->delete($lookup->getLocations()[0], $fid);
	}

	/**
	 *
	 * Delete a file by fid on specified volume server
	 *
	 * @param string $volumeServerAddress
	 * @param string $fid
	 * @return mixed
	 */
	public function delete(string $volumeServerAddress, string $fid) {
		return SC_Http_Request::create($volumeServerAddress . '/' . $fid, SC_Http_Request::METHOD_DELETE)
			->send()->getJson();
	}

	/**
	 * @param string $volumeId
	 * @param string|null $collection
	 * @return bool
	 * @throws SC_Seaweed_Exception
	 */
	public function exists(string $volumeId, ?string $collection = null) {
		try {
			$this->lookup($volumeId, $collection);
			return true;
		}
		catch (SC_Seaweed_Exception $e) {
			if ($e->getCode() == SC_Seaweed_Exception::CODE_NOT_FOUND) {
				return false;
			}

			throw $e;
		}
	}

	/**
	 *
	 * Lookup locations for specified volume by id
	 *
	 * @param string $volumeId
	 * @param string|null $collection
	 * @return \SC\Seaweed\Result\Lookup
	 */
	public function rawLookup(string $volumeId, ?string $collection = null) {
		$url = '/dir/lookup?volumeId=' . $volumeId;
		if ($collection !== null) {
			$url .= "&collection=" . rawurlencode($collection);
		}
		$lookup = $this->doMasterGetRequest($url);

		// {"locations":[{"publicUrl":"localhost:8080","url":"localhost:8080"}]}
		return $lookup;
	}

	/**
	 *
	 * Lookup locations for specified volume by id
	 *
	 * @param string $volumeId
	 * @param string|null $collection
	 * @return \SC\Seaweed\Result\Lookup
	 * @throws SC_Seaweed_Exception
	 */
	public function lookup(string $volumeId, ?string $collection = null) {
		$key = "seaweed_lookup_{$volumeId}_{$collection}";
		$lookup = $this->cache->get($key);
		if ($lookup === false) {
			$lookup = $this->rawLookup($volumeId, $collection);
			$this->cache->set($key, $lookup, 10);
		}

		// {"locations":[{"publicUrl":"localhost:8080","url":"localhost:8080"}]}

		return $this->getLookupResult($lookup, $volumeId, $collection);
	}

	private function getLookupResult($lookup, string $volumeId, ?string $collection = null) {
		try {
			return new \SC\Seaweed\Result\Lookup($lookup);
		}
		catch (SC_Seaweed_Exception $e) {
			throw new SC_Seaweed_Exception("Error in lookup on collection=$collection, volumeId=$volumeId: ".$e->getMessage(), 0, $e);
		}
	}

	/**
	 *
	 * This will assign $count volumes with $replication replication.
	 *
	 * for replication options see:
	 * http://code.google.com/p/weed-fs/#Rack-Aware_and_Data_Center-Aware_Replication
	 *
	 * @param int $count number of volumes
	 * @param string $replication something like 001
	 * @return mixed
	 */
	public function grow(int $count, ?string $replication) {
		$url = '/vol/grow';
		$url .= '?count=' . $count;
		$url .= '&replication=' . $replication;
		return $this->doMasterGetRequest($url);
	}

	/**
	 * Retrieve a file from a specific volume server by fid
	 *
	 * @param string $volumeServerAddress
	 * @param string $fid
	 * @return null|SC_Tempfile
	 * @throws SC_Seaweed_Exception
	 */
	public function retrieve(string $volumeServerAddress, string $fid) {
		$url = $volumeServerAddress . '/' . $fid;
		$down = new SC_Http_Download();
		try {
			return $down->downloadAsFile($url);
		}
		catch (SC_Http_Download_NotFoundException $e) {
			throw new SC_Seaweed_Exception("Fid $fid not found on $volumeServerAddress", SC_Seaweed_Exception::CODE_NOT_FOUND);
		}
		catch (SC_Http_Download_Exception $e) {
			throw new SC_Seaweed_Exception("Unable to retrieve $fid from $volumeServerAddress", SC_Seaweed_Exception::CODE_CONNECT_ERROR);
		}
	}

	public function getHeader(string $volumeServerAddress, string $fid) {
		$url = $volumeServerAddress . '/' . $fid;
		$req = new SC_Http_Transport_Curl();
		return $req->getHeader(new SC_Http_Request($url));
	}

	public function getLastModified(string $volumeServerAddress, string $fid) {
		$header = $this->getHeader($volumeServerAddress, $fid);
		foreach ($header as $line) {
			if (strpos(strtolower($line), "last-modified") === 0) {
				list($key, $value) = explode(': ', $line, 2);
				return new DateTime($value);
			}
		}

		return null;
	}

	public function fileExists(string $volumeServerAddress, string $fid) {
		$headers = $this->getHeader($volumeServerAddress, $fid);

		if (empty($headers)) {
			throw new SC_Seaweed_Exception("Unable to retrieve $fid from $volumeServerAddress, empty headers", SC_Seaweed_Exception::CODE_CONNECT_ERROR);
		}

		$firstLine = $headers[0];
		list($protocol, $status, $message) = explode(' ', $firstLine);
		switch ($status) {
			case '200':
				return true;
			case '404':
			case '302':
				return false;
			default:
				throw new SC_Seaweed_Exception("Unable to retrieve $fid from $volumeServerAddress: {$headers[0]}", SC_Seaweed_Exception::CODE_CONNECT_ERROR);
		}
	}

	public function clusterStatus() {
		return $this->doMasterGetRequest('/cluster/status');
	}

	public function status() {
		return $this->doMasterGetRequest('/dir/status');
	}

	public function volumeStatus() {
		return $this->doMasterGetRequest('/vol/status');
	}

	public function volumeServerStatus(string $volumeServerAddress) {
		return $this->doGetRequest($volumeServerAddress . '/status');
	}

	public function volumeServerStatsDisk(string $volumeServerAddress) {
		return $this->doGetRequest($volumeServerAddress . '/stats/disk');
	}

	public function volumeMount(string $volumeServerAddress, int $volumeId) {
		return $this->doGetRequest($volumeServerAddress . '/admin/volume/mount?volume='.$volumeId);
	}

	public function volumeUnmount(string $volumeServerAddress, int $volumeId) {
		return $this->doGetRequest($volumeServerAddress . '/admin/volume/unmount?volume='.$volumeId);
	}

	public function volumeDelete(string $volumeServerAddress, int $volumeId) {
		return $this->doGetRequest($volumeServerAddress . '/admin/volume/delete?volume='.$volumeId);
	}

	/**
	 *
	 * Store multiple files at once, assuming you have assigned the same number of count for fid
	 * as you have number of files.
	 *
	 * @param string $volumeServerAddress
	 * @param string $fid base fid for all files
	 * @param array $files
	 * @param string|null $mimeType
	 * @param string|null $ttl
	 * @return mixed
	 */
	public function storeMultiple(string $volumeServerAddress, string $fid, array $files, ?string $mimeType = null, ?string $ttl = null) {
		$count = count($files);

		$storeUrl = $this->buildStoreUrl($volumeServerAddress, $fid, $ttl);
		$response = [];
		for ($i = 1; $i <= $count; $i++) {
			$response[] = SC_Http_Request::create($storeUrl, SC_Http_Request::METHOD_POST)
				->addPostFile('file', $files[$i - 1], $mimeType)
				->send();

			$storeUrl = $this->buildStoreUrl($volumeServerAddress, $fid . '_' . $i, $ttl);
		}

		return $response;
	}

	protected function buildStoreUrl(string $volumeServerAddress,
	                                 string $fid,
	                                 ?string $ttl = null,
	                                 $noReplicate = false,
	                                 ?DateTime $lastModified = null
	) {
		$storeUrl = $volumeServerAddress . '/' . $fid;
		$queryData = [];
		if ($ttl !== null) {
			$queryData['ttl'] = $ttl;
		}

		if ($noReplicate) {
			$queryData['type'] = 'replicate';
		}

		if ($lastModified !== null) {
			$queryData['ts'] = $lastModified->getTimestamp();
		}

		$query = http_build_query($queryData);

		return $storeUrl . (!empty($query) ? '?' . $query : '');
	}

	protected function buildStoreRequest(string $storeUrl, string $file, ?string $mimeType = null) {
		return SC_Http_Request::create($storeUrl, SC_Http_Request::METHOD_POST)
			->addPostFile('file', $file, $mimeType);
	}

	/**
	 *
	 * Store a single file on volume server. Use assign first to get the volume server
	 * and fid
	 *
	 * @param string $volumeServerAddress
	 * @param string $fid
	 * @param string $file
	 * @param string|null $mimeType
	 * @param string|null $ttl
	 * @return SC_Http_Response
	 * @throws SC_Seaweed_Exception
	 */
	public function store(string $volumeServerAddress, string $fid, string $file, ?string $mimeType = null, ?string $ttl = null) {
		$storeUrl = $this->buildStoreUrl($volumeServerAddress, $fid, $ttl);
		$result = $this->buildStoreRequest($storeUrl, $file, $mimeType)->send();
		if ($result->getCode() !== 201) {
			throw new SC_Seaweed_Exception("Upload failed: (Code {$result->getCode()}) ".$result->getBody());
		}

		return $result;
	}

	/**
	 *
	 * Store a single file on volume server. Do not replicate on other hosts
	 *
	 * @param string $volumeServerAddress
	 * @param string $fid
	 * @param string $file
	 * @param string|null $mimeType
	 * @param string|null $ttl
	 * @param DateTime|null $lastModified
	 * @return SC_Http_Response
	 */
	public function storeNoReplicate(string $volumeServerAddress,
	                                 string $fid,
	                                 string $file,
	                                 ?string $mimeType = null,
	                                 ?string $ttl = null,
	                                 ?DateTime $lastModified = null
	) {
		$storeUrl = $this->buildStoreUrl($volumeServerAddress, $fid, $ttl, true, $lastModified);
		return $this->buildStoreRequest($storeUrl, $file, $mimeType)->send();
	}

}
