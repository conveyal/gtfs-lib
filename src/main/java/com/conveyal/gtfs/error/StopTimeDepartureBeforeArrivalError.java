package com.conveyal.gtfs.error;

import com.conveyal.gtfs.validator.model.Priority;

import java.io.Serializable;

/**
 * Created by landon on 5/6/16.
 */
public class StopTimeDepartureBeforeArrivalError extends GTFSError implements Serializable {
    public static final long serialVersionUID = 1L;

    public final Priority priority = Priority.HIGH;
    public final String tripId;
    public final int stopSequence;

    public StopTimeDepartureBeforeArrivalError(String tripId, int stopSequence) {
        super("stop_times", 0, "trip_id");
        this.tripId = tripId;
        this.stopSequence = stopSequence;

    }

    @Override public String getMessage() {
        return "Trip Id " + tripId + " stop sequence " + stopSequence + " departs before arriving.";
    }
}
