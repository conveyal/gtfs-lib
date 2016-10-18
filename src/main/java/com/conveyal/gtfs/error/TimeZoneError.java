package com.conveyal.gtfs.error;

import com.conveyal.gtfs.model.Agency;

import java.io.Serializable;

/**
 * Created by landon on 5/11/16.
 */
public class TimeZoneError extends GTFSError implements Serializable {
    public static final long serialVersionUID = 1L;

    public final String message;
    public final String affectedEntityId;
    public TimeZoneError(String tableName, long line, String field, String affectedEntityId, String message) {
        super(tableName, line, field);
        this.message = message;
        this.affectedEntityId = affectedEntityId;
    }

    @Override public String getMessage() {
        return message + ". (" + field + ": " + affectedEntityId + ")";
    }
}
