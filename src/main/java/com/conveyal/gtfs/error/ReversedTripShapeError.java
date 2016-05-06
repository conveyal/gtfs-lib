package com.conveyal.gtfs.error;

import com.conveyal.gtfs.validator.model.Priority;

/**
 * Created by landon on 5/6/16.
 */
public class ReversedTripShapeError extends GTFSError {

    public Priority priority = Priority.HIGH;
    public String tripId;
    public String shapeId;

    public ReversedTripShapeError(String tripId, String shapeId) {
        super("trip", 0, "shape_id");
        this.tripId = tripId;
        this.shapeId = shapeId;
    }

    @Override public String getMessage() {
        return "Trip " + tripId + " references reversed shape " + shapeId;
    }
}
