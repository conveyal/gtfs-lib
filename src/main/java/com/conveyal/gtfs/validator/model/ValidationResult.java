package com.conveyal.gtfs.validator.model;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.error.GTFSError;

import java.io.Serializable;
import java.util.Date;
import java.util.NavigableSet;


public class ValidationResult implements Serializable {
	public String fileName;
	public String validationTimestamp;
	public long errorCount;
	public NavigableSet<GTFSError> errors;

	public ValidationResult (String fileName, GTFSFeed feed) {
		this.fileName = fileName;
		this.validationTimestamp = new Date().toString();
		this.errorCount = feed.errors.size();
		this.errors = feed.errors;
	}
}
