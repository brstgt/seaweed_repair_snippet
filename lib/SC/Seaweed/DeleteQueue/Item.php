<?php

namespace SC\Seaweed\DeleteQueue;

use DateTime;

class Item {

	protected $fid;

	protected $collection;

	protected $replicationCount;

	protected $tryCount;

	/**
	 * @var DateTime
	 */
	protected $retryAt;

	public function __construct($row) {
		$this->fid = $row['file_id'];
		$this->collection = $row['collection'];
		$this->replicationCount = $row['replication_count'];
		$this->tryCount = (int)$row['try_count'];
		$this->retryAt = new DateTime($row['retry_at']);
	}

	/**
	 * @return mixed
	 */
	public function getFid() {
		return $this->fid;
	}

	/**
	 * @return mixed
	 */
	public function getCollection() {
		return $this->collection;
	}

	/**
	 * @return mixed
	 */
	public function getReplicationCount() {
		return $this->replicationCount;
	}

	/**
	 * @return int
	 */
	public function getTryCount(): int {
		return $this->tryCount;
	}


}