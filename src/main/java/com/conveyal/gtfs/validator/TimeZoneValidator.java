package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.Agency;
import com.conveyal.gtfs.model.Stop;

import java.time.ZoneId;
import java.util.ArrayList;

import static com.conveyal.gtfs.error.NewGTFSErrorType.*;

public class TimeZoneValidator extends FeedValidator {

    public TimeZoneValidator(Feed feed, SQLErrorStorage errorStorage) {
        super(feed, errorStorage);
    }

    @Override
    public void validate() {
        for (Agency agency : new ArrayList<Agency>()) { //feed.agency) {
            if (agency.agency_timezone == null) {
                //FIXME missing fields should already be detected
                continue;
            }
            try {
                ZoneId.of(agency.agency_timezone);
            } catch (Exception ex) {
                registerError(TIME_ZONE_FORMAT, agency.agency_timezone, agency);
            }
        }
        for (Stop stop : feed.stops) {
            // stop_timezone is an optional field. If it is missing, just skip this stop.
            if (stop.stop_timezone == null) continue;
            try {
                ZoneId.of(stop.stop_timezone);
            } catch (Exception ex) {
                registerError(TIME_ZONE_FORMAT, stop.stop_timezone, stop);
            }
        }
    }
}
