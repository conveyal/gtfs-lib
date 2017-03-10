package com.conveyal.gtfs.error;

import com.conveyal.gtfs.model.Trip;
import com.conveyal.gtfs.validator.model.Priority;

import java.io.Serializable;

/**
 * Created by landon on 5/6/16.
 */
public class DuplicateTripError extends GTFSError implements Serializable {
    public static final long serialVersionUID = 1L;

    public final Priority priority = Priority.LOW;
    public final String duplicateTripId;
    public final String tripKey;
    public final String routeId;

    public DuplicateTripError(Trip trip, String duplicateTripId, String tripKey) {
        super("trips", trip.sourceFileLine, "trip_id", trip.trip_id);
        this.duplicateTripId = duplicateTripId;
        this.tripKey = tripKey;
        this.routeId = trip.route_id;

    }

    @Override public String getMessage() {
        return String.format("Trip Ids %s & %s (route %s) are duplicates (%s)", duplicateTripId, affectedEntityId, routeId, tripKey);
    }
}
