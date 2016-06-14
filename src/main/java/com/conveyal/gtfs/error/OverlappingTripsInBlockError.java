package com.conveyal.gtfs.error;

import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.validator.model.Priority;

import java.io.Serializable;

/**
 * Created by landon on 5/6/16.
 */
public class OverlappingTripsInBlockError extends GTFSError implements Serializable {
    public static final long serialVersionUID = 1L;

    public String[] tripIds;
    public String affectedEntityId;
    public Priority priority = Priority.HIGH;

    public OverlappingTripsInBlockError(String file, long line, String field, String affectedEntityId, String[] tripIds, Priority priority) {
        super(file, line, field);
        this.tripIds = tripIds;
        this.affectedEntityId = affectedEntityId;
    }

    @Override public String getMessage() {
        return "Trip Ids " + String.join(" & ", tripIds) + " overlap and share block Id " + affectedEntityId;
    }
}
