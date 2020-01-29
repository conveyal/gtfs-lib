package com.conveyal.gtfs.model;

import com.sleepycat.persist.model.KeyField;
import com.sleepycat.persist.model.Persistent;

import java.util.Objects;

/**
 * A unique key for stop_times: the trip_id and stop_sequence_number.
 */
@Persistent
public class TripAndSequence {

    @KeyField(1)
    String tripId;

    @KeyField(2)
    int stopSequence;

    /** For deserialization only. */
    private TripAndSequence () { }

    public TripAndSequence (String tripId, int stopSequence) {
        this.tripId = tripId;
        this.stopSequence = stopSequence;
    }

    @Override
    public boolean equals (Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TripAndSequence that = (TripAndSequence) o;
        return stopSequence == that.stopSequence && tripId.equals(that.tripId);
    }

    @Override
    public int hashCode () {
        return Objects.hash(tripId, stopSequence);
    }
}
