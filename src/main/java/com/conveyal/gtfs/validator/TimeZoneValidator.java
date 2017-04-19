package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.Agency;
import com.conveyal.gtfs.model.Stop;

import java.time.ZoneId;
import java.util.ArrayList;

import static com.conveyal.gtfs.error.NewGTFSErrorType.*;

public class TimeZoneValidator extends FeedValidator {

    @Override
    public boolean validate(Feed feed, boolean repair) {
        for (Agency agency : new ArrayList<Agency>()) { //feed.agency) {
            if (agency.agency_timezone == null) {
                registerError(NO_TIME_ZONE, agency.agency_timezone, agency); // Required Field
                continue;
            }
            try {
                ZoneId.of(agency.agency_timezone);
            } catch (Exception ex) {
                registerError(BAD_TIME_ZONE, agency.agency_timezone, agency);
            }
        }
        for (Stop stop : feed.stops) {
            // stop_timezone is an optional field. If it is missing, just skip this stop.
            if (stop.stop_timezone == null) continue;
            try {
                ZoneId.of(stop.stop_timezone);
            } catch (Exception ex) {
                registerError(BAD_TIME_ZONE, stop.stop_timezone, stop);
            }
        }
        return false;
    }
}
