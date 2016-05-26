package com.conveyal.gtfs.error;

import com.conveyal.gtfs.validator.model.Priority;

/**
 * Created by landon on 5/6/16.
 */
public class StopTimeDepartureBeforeArrivalError extends GTFSError {

    public Priority priority = Priority.HIGH;
    public String tripId;
    public int stopSequence;

    public StopTimeDepartureBeforeArrivalError(String tripId, int stopSequence) {
        super("stop_times", 0, "trip_id");
        this.tripId = tripId;
        this.stopSequence = stopSequence;

    }

    @Override public String getMessage() {
        return "Trip Id " + tripId + " stop sequence " + stopSequence + " departs before arriving.";
    }
}
