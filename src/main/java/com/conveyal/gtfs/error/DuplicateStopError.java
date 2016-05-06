package com.conveyal.gtfs.error;

import com.conveyal.gtfs.validator.model.DuplicateStops;

/** Indicates that a stop exists more than once in the feed. */
public class DuplicateStopError extends GTFSError {

    private String message;

    public DuplicateStopError(String file, long line, String field, String message, DuplicateStops duplicateStop) {
        super(file, line, field);
    }

    @Override public String getMessage() {
        return message;
    }

}
