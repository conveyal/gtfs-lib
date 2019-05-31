package com.conveyal.gtfs.storage;

import com.conveyal.gtfs.error.NewGTFSErrorType;
import org.hamcrest.Matcher;

import static org.hamcrest.Matchers.equalTo;

/**
 * Defines the expected values for an error stored in the errors table for a feed in the GTFS database.
 *
 * Note: the errors should be listed in order that they are expected to be encountered. Check out
 * {@link com.conveyal.gtfs.loader.JdbcGtfsLoader#loadTables} to see the order in which tables are loaded,
 * {@link com.conveyal.gtfs.loader.Feed#validate()} to see the order in which validators are called (trip validator
 * order can be found in {@link com.conveyal.gtfs.validator.NewTripTimesValidator}).
 */
public class ErrorExpectation {
    public Matcher<String> errorTypeMatcher;
    public Matcher<String> badValueMatcher;
    public Matcher<String> entityTypeMatcher;
    public Matcher<String> entityIdMatcher;

    public ErrorExpectation(NewGTFSErrorType errorType) {
        this(errorType, null, null, null);
    }

    public ErrorExpectation(NewGTFSErrorType errorType, Matcher<String> entityIdMatcher) {
        this(errorType, null, null, entityIdMatcher);
    }

    /**
     * Note: we accept Matchers as constructor args rather than the actual string values because this gives us the
     * ability to specify null values in the case that we don't care about matching a specific value for an error
     * (e.g., we only want to check for a matching error type but are not concerned with a specific error's entity ID
     * value).
     */
    public ErrorExpectation(NewGTFSErrorType errorType, Matcher<String> badValueMatcher, Matcher<String> entityTypeMatcher, Matcher<String> entityIdMatcher) {
        this.errorTypeMatcher = equalTo(errorType.toString());
        this.badValueMatcher = badValueMatcher;
        this.entityTypeMatcher = entityTypeMatcher;
        this.entityIdMatcher = entityIdMatcher;
    }

    /**
     * Constructs an array of error expectations from the input args.
     */
    public static ErrorExpectation[] list (ErrorExpectation... expectations) {
        return expectations;
    }
}
