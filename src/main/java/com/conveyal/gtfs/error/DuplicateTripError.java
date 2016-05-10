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
    public Route route;

    public DuplicateTripError(String tripId, String duplicateTripId, String tripKey, Route route) {
        super("trip", 0, "trip_id");
        this.tripId = tripId;
        this.duplicateTripId = duplicateTripId;
        this.tripKey = tripKey;
        this.route = route;

    }

    @Override public String getMessage() {
        return "Trip Ids " + duplicateTripId + " & " + tripId + " are duplicates (" + tripKey + ")";
    }
}
