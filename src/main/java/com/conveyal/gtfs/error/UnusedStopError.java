package com.conveyal.gtfs.error;

import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.validator.model.Priority;

/** Indicates that a stop exists more than once in the feed. */
public class UnusedStopError extends GTFSError {

    public String affectedEntityId;
    public Priority priority;
    public Stop stop;

    public UnusedStopError(String affectedEntityId, Stop stop) {
        super("stops", 0, "stop_id");
        this.affectedEntityId = affectedEntityId;
        this.priority = Priority.LOW;
        this.stop = stop;
    }

    @Override public String getMessage() {
        return String.format("Stop Id %s is not used in any trips.", affectedEntityId);
    }
}
