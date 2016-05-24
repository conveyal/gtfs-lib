package com.conveyal.gtfs.error;

import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.validator.model.Priority;

/**
 * Created by landon on 5/6/16.
 */
public class DuplicateTripError extends GTFSError {

    public Priority priority = Priority.LOW;
    public String tripId;
    public String duplicateTripId;
    public String tripKey;
    public String routeId;

    public DuplicateTripError(String tripId, String duplicateTripId, String tripKey, String routeId) {
        super("trip", 0, "trip_id");
        this.tripId = tripId;
        this.duplicateTripId = duplicateTripId;
        this.tripKey = tripKey;
        this.routeId = routeId;

    }

    @Override public String getMessage() {
        return "Trip Ids " + duplicateTripId + " & " + tripId + " are duplicates (" + tripKey + ")";
    }
}
