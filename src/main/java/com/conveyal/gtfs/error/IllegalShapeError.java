package com.conveyal.gtfs.error;

import com.conveyal.gtfs.validator.model.Priority;

/**
 * Created by landon on 5/6/16.
 */
public class IllegalShapeError extends GTFSError {
    private String entityId;

    public IllegalShapeError(String file, long line, String field, String entityId, Priority priority) {
        super(file, line, field);
        this.entityId = entityId;
    }

    @Override public String getMessage() {
        return String.format("Illegal stopCoord for shape %s.", entityId);
    }
}
