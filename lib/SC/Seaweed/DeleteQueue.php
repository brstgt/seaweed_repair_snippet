<?php

namespace SC\Seaweed;

use DateTime;
use SC\Seaweed\DeleteQueue\Item;
use Sensphere_DB_ConnectionPoolManager;
use Sensphere_DB_MySql_Connection;
use Sensphere_Log;
use Throwable;

class DeleteQueue {

	/**
	 * in minutes
	 *
	 * @var int
	 */
	const BACKOFF_TIME = 1;

	/**
	 * @var float
	 */
	const BACKOFF_MULTIPLICATOR = 1.5;

	/**
	 * @var Sensphere_Log
	 */
	protected $logger;

	/**
	 * @var DateTime
	 */
	protected $now;

	public function __construct() {
		$this->logger = Sensphere_Log::create('seaweed');
	}

	/**
	 * @param DateTime $now
	 */
	public function setNow(DateTime $now) {
		$this->now = $now;
	}

	/**
	 * @return DateTime
	 */
	public function getNow(): DateTime {
		if ($this->now !== null) {
			return clone $this->now;
		}
		return new DateTime();
	}

	public function enqueue(string $fid, string $collection, string $replicationCount) {
		$db = $this->getReplayLogDb();

		$date = $this->getNow();
		$date->add(new \DateInterval("PT" . self::BACKOFF_TIME . "M"));

		$query = 'INSERT INTO
			seaweed_delete_queue
		SET
			file_id = ' . $db->quote($fid) . ',
			collection = ' . $db->quote($collection) . ',
			replication_count = ' . $db->quote($replicationCount) . ',
			enqueued_at = NOW(),
			status_change = NOW(),
			retry_at = ' . $db->quote($date) . ',
			try_count = 0';

		$db->query($query);
	}

	public function requeue(string $fid, int $tryCount, Throwable $exception) {
		$tryCount++;
		$db = $this->getReplayLogDb();
		$backOffTime = self::BACKOFF_TIME;
		for ($x = 0; $x < $tryCount; $x++) {
			$backOffTime *= self::BACKOFF_MULTIPLICATOR;
		}
		$date = $this->getNow();
		$date->add(new \DateInterval("PT" . (int)($backOffTime * 60). "S"));

		$query = 'UPDATE
			seaweed_delete_queue
		SET
			status_change = NOW(),
			exception = ' . $db->quote($exception->getMessage() . "\n" . $exception->getTraceAsString()) . ',
			try_count = ' . $db->quote($tryCount) . ',
			retry_at = ' . $db->quote($date) . '
		WHERE file_id = ' . $db->quote($fid);

		$db->query($query);
	}

	/**
	 * @param int $limit
	 * @return DeleteQueue\Item[]
	 */
	public function pop($limit = 100) {
		$db = $this->getReplayLogDb();
		$query = 'SELECT
					file_id, collection, replication_count, try_count, retry_at
				FROM
					seaweed_delete_queue
				WHERE
					retry_at <= ' . $db->quote($this->getNow()) . '
				LIMIT ' . $limit;

		$result = $db->query($query);
		$items = [];
		while ($row = $result->fetch_assoc()) {
			$items[] = new Item($row);
		}
		return $items;
	}

	public function get(string $fid) {
		$db = $this->getReplayLogDb();
		$query = 'SELECT 
 					file_id, collection, replication_count, try_count, retry_at
				FROM
					seaweed_delete_queue
				WHERE
					file_id = ' . $db->quote($fid);
		$r = new \Sensphere_DB_MySql_Result($db->query($query));
		return new Item($r->getRow());
	}

	public function dequeue(string $fid) {
		$db = $this->getReplayLogDb();
		$query = 'DELETE FROM
					seaweed_delete_queue
				WHERE
					file_id = ' . $db->quote($fid);
		$db->query($query);
	}

	public function count() {
		$db = $this->getReplayLogDb();
		$query = 'SELECT count(*) FROM seaweed_delete_queue';
		$r = new \Sensphere_DB_MySql_Result($db->query($query));
		return $r->getOne();
	}

	public function flush() {
		$db = $this->getReplayLogDb();
		$query = 'TRUNCATE TABLE seaweed_delete_queue';
		$db->query($query);
	}

	/**
	 * @return Sensphere_DB_MySql_Connection
	 */
	protected function getReplayLogDb() {
		return Sensphere_DB_ConnectionPoolManager::getInstance()->getConnectionPool('log')->getConnectionSync();
	}

	protected function logError($error) {
		$this->logger->error($error);
	}

	protected function log(\Throwable $exception) {
		$this->logger->exception($exception);
	}
}
