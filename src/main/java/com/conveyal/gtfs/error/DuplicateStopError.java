package com.conveyal.gtfs.error;

import com.conveyal.gtfs.validator.model.DuplicateStops;

import java.io.Serializable;

/** Indicates that a stop exists more than once in the feed. */
public class DuplicateStopError extends GTFSError implements Serializable {
    public static final long serialVersionUID = 1L;

    private final String message;
    public final DuplicateStops duplicateStop;

    public DuplicateStopError(DuplicateStops duplicateStop) {
        super("stop", duplicateStop.stop1.sourceFileLine, "stop_lat,stop_lon", duplicateStop.getStop1Id());
        this.message = duplicateStop.toString();
        this.duplicateStop = duplicateStop;
    }

    @Override public String getMessage() {
        return message;
    }

}
