package com.conveyal.gtfs.error;

import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.validator.model.Priority;

import java.io.Serializable;

/**
 * Created by landon on 5/2/16.
 */
public class StopMissingCoordinatesError extends GTFSError implements Serializable {
    public static final long serialVersionUID = 1L;

    public final Priority priority = Priority.MEDIUM;
    public final Stop stop;
    public StopMissingCoordinatesError(Stop stop) {
        super("stops", stop.sourceFileLine, "stop_lat,stop_lon", stop.stop_id);
        this.stop = stop;
    }

    @Override public String getMessage() {
        return "Stop " + affectedEntityId + " is missing coordinates";
    }
}
