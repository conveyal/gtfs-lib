package com.conveyal.gtfs.error;

import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.validator.model.Priority;

import java.io.Serializable;

/**
 * Created by landon on 5/6/16.
 */
public class StopTimesOutOfSequenceError extends GTFSError implements Serializable {
    public static final long serialVersionUID = 1L;

    public final Priority priority = Priority.HIGH;
    public final int stopSequence;
    public final int previousStopSequence;

    public StopTimesOutOfSequenceError(StopTime stopTime, StopTime previousStopTime) {
        super("stop_times", stopTime.sourceFileLine, "trip_id", stopTime.trip_id);
        this.stopSequence = stopTime.stop_sequence;
        this.previousStopSequence = previousStopTime.stop_sequence;

    }

    @Override public String getMessage() {
        return "Trip Id " + affectedEntityId + " stop sequence " + stopSequence + " arrives before departing " + previousStopSequence;
    }
}
