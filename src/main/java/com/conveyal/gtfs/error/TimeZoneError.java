package com.conveyal.gtfs.error;

import com.conveyal.gtfs.model.Agency;

import java.io.Serializable;

/**
 * Created by landon on 5/11/16.
 */
public class TimeZoneError extends GTFSError implements Serializable {
    public static final long serialVersionUID = 1L;

    public Agency agency;
    public String message;
    public String affectedEntityId;
    public TimeZoneError(long line, Agency agency, String message) {
        super("agency", line, "agency_timezone");
        this.agency = agency;
        this.message = message;
        this.affectedEntityId = agency.agency_id;
    }

    @Override public String getMessage() {
        return message + ". (agency: " + agency.agency_name + ")";
    }
}
