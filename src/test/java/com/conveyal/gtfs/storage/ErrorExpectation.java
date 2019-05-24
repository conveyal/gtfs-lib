package com.conveyal.gtfs.storage;

import com.conveyal.gtfs.error.NewGTFSErrorType;

/**
 * Defines the expected values for an error stored in the errors table for a feed in the GTFS database.
 *
 * Note: the errors should be listed in order that they are expected to be encountered. Check out
 * {@link com.conveyal.gtfs.loader.JdbcGtfsLoader#loadTables} to see the order in which tables are loaded,
 * {@link com.conveyal.gtfs.loader.Feed#validate()} to see the order in which validators are called (trip validator
 * order can be found in {@link com.conveyal.gtfs.validator.NewTripTimesValidator}).
 */
public class ErrorExpectation {
    public NewGTFSErrorType errorType;
    public String badValue;
    public String entityType;
    public String entityId;

    public ErrorExpectation(NewGTFSErrorType errorType) {
        this.errorType = errorType;
    }

    public ErrorExpectation(NewGTFSErrorType errorType, String entityId) {
        this.errorType = errorType;
        this.entityId = entityId;
    }

    public ErrorExpectation(NewGTFSErrorType errorType, String badValue, String entityType, String entityId) {
        this.errorType = errorType;
        this.badValue = badValue;
        this.entityType = entityType;
        this.entityId = entityId;
    }

    /**
     * Constructs an array of error expectations from the input args.
     */
    public static ErrorExpectation[] list (ErrorExpectation... expectations) {
        return expectations;
    }
}
