package com.conveyal.gtfs.validator.model;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.error.GTFSError;
import com.conveyal.gtfs.model.FeedInfo;
import com.conveyal.gtfs.stats.FeedStats;
import com.conveyal.gtfs.stats.model.FeedStatistic;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.Date;
import java.util.List;
import java.util.NavigableSet;
import java.util.stream.Collectors;


public class ValidationResult implements Serializable {
	public String fileName;
	public String validationTimestamp;
	public FeedStatistic feedStatistics;
	public long errorCount;
	public NavigableSet<GTFSError> errors;

	public ValidationResult (String fileName, GTFSFeed feed, FeedStats feedStats) {
		this.fileName = fileName;
		this.validationTimestamp = new Date().toString();
		this.feedStatistics = new FeedStatistic(feedStats);
		this.errorCount = feed.errors.size();
		this.errors = feed.errors;
	}
}
