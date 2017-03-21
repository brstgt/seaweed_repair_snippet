<?php

namespace SC\Seaweed\Result;

class Assign {

	protected $jsonResult;

	public function __construct($jsonResult) {
		$this->jsonResult = $jsonResult;
	}

	public function getFid() {
		return $this->jsonResult->fid;
	}

	public function getCount() {
		return $this->jsonResult->count;
	}

	public function getPublicUrl() {
		return "http://".$this->jsonResult->publicUrl;
	}

}