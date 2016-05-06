package com.conveyal.gtfs.error;

import com.conveyal.gtfs.validator.model.Priority;

/** Indicates that a stop exists more than once in the feed. */
public class UnusedStopError extends GTFSError {

    private String affectedEntityId;
    private Priority priority;

    public UnusedStopError(String file, long line, String field, String affectedEntityId, Priority priority) {
        super(file, line, field);
        this.affectedEntityId = affectedEntityId;
        this.priority = priority;
    }

    @Override public String getMessage() {
        return String.format("Stop Id %s is not used in any trips.", affectedEntityId);
    }

}
