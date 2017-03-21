<?php

namespace SC\Seaweed\Result;

class Lookup {

	protected $jsonResult;

	public function __construct($jsonResult) {
		$this->jsonResult = $jsonResult;
		if (empty($this->jsonResult->locations) || !is_array($this->jsonResult->locations)) {
			throw new \SC_Seaweed_Exception("Locations missing in lookup result");
		}
	}

	public function getLocations() {
		$locations = [];

		foreach ($this->jsonResult->locations as $location) {
			$locations[] = "http://".$location->publicUrl;
		}

		return $locations;
	}

}