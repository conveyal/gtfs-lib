package com.conveyal.gtfs.error;

import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.validator.model.Priority;

import java.io.Serializable;

/**
 * Created by landon on 5/2/16.
 */
public class StopMissingCoordinatesError extends GTFSError implements Serializable {
    public static final long serialVersionUID = 1L;

    public Priority priority = Priority.MEDIUM;
    public String stopId;
    public Stop stop;
    public StopMissingCoordinatesError(String stopId, Stop stop) {
        super("stops", 0, "stop_lat,stop_lon");
        this.stopId = stopId;
        this.stop = stop;
    }

    @Override public String getMessage() {
        return "Stop " + stopId + " is missing coordinates";
    }
}
