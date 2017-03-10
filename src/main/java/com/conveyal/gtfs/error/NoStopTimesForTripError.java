package com.conveyal.gtfs.error;

import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.Trip;
import com.conveyal.gtfs.validator.model.Priority;

import java.io.Serializable;

/**
 * Created by landon on 5/6/16.
 */
public class NoStopTimesForTripError extends GTFSError implements Serializable {
    public static final long serialVersionUID = 1L;

    public final Priority priority = Priority.HIGH;
    public final String routeId;

    public NoStopTimesForTripError(Trip trip) {
        super("trip", trip.sourceFileLine, "trip_id", trip.trip_id);
        this.routeId = trip.route_id;
    }

    @Override public String getMessage() {
        return String.format("Trip Id %s (route: %s) has no stop times.", affectedEntityId, routeId);
    }
}
