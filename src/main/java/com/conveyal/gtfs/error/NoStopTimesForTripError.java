package com.conveyal.gtfs.error;

import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.validator.model.Priority;

/**
 * Created by landon on 5/6/16.
 */
public class NoStopTimesForTripError extends GTFSError {

    public Priority priority = Priority.HIGH;
    public String tripId;
    public Route route;

    public NoStopTimesForTripError(String tripId, Route route) {
        super("trips", 0, "trip_id");
        this.tripId = tripId;
        this.route = route;
    }

    @Override public String getMessage() {
        return "Trip Id " + tripId + " has no stop times.";
    }
}
