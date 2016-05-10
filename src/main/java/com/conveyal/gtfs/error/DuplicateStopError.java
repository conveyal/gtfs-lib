package com.conveyal.gtfs.error;

import com.conveyal.gtfs.validator.model.DuplicateStops;

/** Indicates that a stop exists more than once in the feed. */
public class DuplicateStopError extends GTFSError {

    private String message;
    public DuplicateStops duplicateStop;
    public DuplicateStopError(String message, DuplicateStops duplicateStop) {
        super("stop", 0, "stop_lat,stop_lon");
        this.message = message;
        this.duplicateStop = duplicateStop;

    }

    @Override public String getMessage() {
        return message;
    }

}
