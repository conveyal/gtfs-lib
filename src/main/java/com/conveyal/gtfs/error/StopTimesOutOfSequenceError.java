package com.conveyal.gtfs.error;

import com.conveyal.gtfs.validator.model.Priority;

/**
 * Created by landon on 5/6/16.
 */
public class StopTimesOutOfSequenceError extends GTFSError {

    public Priority priority = Priority.HIGH;
    public String tripId;
    public int stopSequence;
    public int previousStopSequence;

    public StopTimesOutOfSequenceError(String tripId, int stopSequence, int previousStopSequence) {
        super("stop_time", 0, "trip_id");
        this.tripId = tripId;
        this.stopSequence = stopSequence;
        this.previousStopSequence = previousStopSequence;

    }

    @Override public String getMessage() {
        return "Trip Id " + tripId + " stop sequence " + stopSequence + " arrives before departing " + previousStopSequence;
    }
}
