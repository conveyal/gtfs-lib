package com.conveyal.gtfs.error;

import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.validator.model.Priority;

/** Indicates that a stop exists more than once in the feed. */
public class UnusedStopError extends GTFSError {

    private String affectedEntityId;
    private Priority priority;

    public UnusedStopError(String affectedEntityId, Stop stop, Priority priority) {
        super("stop", 0, "stop_id");
        this.affectedEntityId = affectedEntityId;
        this.priority = priority;
    }

    @Override public String getMessage() {
        return String.format("Stop Id %s is not used in any trips.", affectedEntityId);
    }

}
