package com.conveyal.gtfs.error;

import com.conveyal.gtfs.validator.model.Priority;

import java.io.Serializable;

/**
 * Created by landon on 5/2/16.
 */
public class ShapeMissingCoordinatesError extends GTFSError implements Serializable {
    public static final long serialVersionUID = 1L;

    public final Priority priority = Priority.MEDIUM;
    public final String[] tripIds;
    public final String shapeId;

    public ShapeMissingCoordinatesError(String shapeId, String[] tripIds) {
        super("shapes", 0, "shape_id");
        this.tripIds = tripIds;
        this.shapeId = shapeId;
    }

    @Override public String getMessage() {
        return "Shape " + shapeId + " is missing coordinates (affects " + tripIds.length + " trips)";
    }
}