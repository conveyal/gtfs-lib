package com.conveyal.gtfs.error;

import java.io.Serializable;

/** Represents a problem parsing location GeoJson from a GTFS feed. */
public class GeoJsonParseError extends GTFSError implements Serializable {
    public static final long serialVersionUID = 1L;

    public final String message;

    public GeoJsonParseError(String file, String message) {
        super(file, 0, null);
        this.message = message;
    }

    @Override
    public String getMessage() {
        return message;
    }
}
