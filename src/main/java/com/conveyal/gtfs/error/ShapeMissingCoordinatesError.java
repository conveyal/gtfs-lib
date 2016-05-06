package com.conveyal.gtfs.error;

import com.conveyal.gtfs.validator.model.Priority;

/**
 * Created by landon on 5/2/16.
 */
public class ShapeMissingCoordinatesError extends GTFSError {

    public Priority priority = Priority.MEDIUM;
    public String tripId;
    public String shapeId;

    public ShapeMissingCoordinatesError(String tripId, String shapeId) {
        super("trip", 0, "shape_id");
        this.tripId = tripId;
        this.shapeId = shapeId;
    }

    @Override public String getMessage() {
        return "Trip " + tripId + " (shape " + shapeId + ") is missing coordinates";
    }
}