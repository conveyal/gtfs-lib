package com.conveyal.gtfs.error;

import com.conveyal.gtfs.validator.model.Priority;

/**
 * Created by landon on 5/2/17.
 */
public class NoOperatorInFeedError extends GTFSError {
    public final Priority priority = Priority.HIGH;

    public NoOperatorInFeedError() {
        super("agency", 0, "agency_id");
    }

    @Override public String getMessage() {
        return String.format("No operator listed in feed (must have at least one).");
    }
}
