package com.conveyal.gtfs.error;

import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.validator.model.Priority;

import java.io.Serializable;

/**
 * Created by landon on 5/6/16.
 */
public class NoStopTimesForTripError extends GTFSError implements Serializable {
    public static final long serialVersionUID = 1L;

    public Priority priority = Priority.HIGH;
    public String tripId;
    public String routeId;

    public NoStopTimesForTripError(String tripId, String routeId) {
        super("trip", 0, "trip_id");
        this.tripId = tripId;
        this.routeId = routeId;
    }

    @Override public String getMessage() {
        return "Trip Id " + tripId + " has no stop times.";
    }
}
