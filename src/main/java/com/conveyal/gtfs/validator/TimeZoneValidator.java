package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.error.TimeZoneError;
import com.conveyal.gtfs.model.Agency;
import com.conveyal.gtfs.model.Stop;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.time.zone.ZoneRulesException;

/**
 * Created by landon on 5/11/16.
 */
public class TimeZoneValidator extends GTFSValidator {
    @Override
    public boolean validate(GTFSFeed feed, boolean repair) {
        int index = 0;
        for (Agency agency : feed.agency.values()) {
            index++;
            if (agency.agency_timezone == null) {
                feed.errors.add(new TimeZoneError("agency", agency.sourceFileLine, "agency_timezone", agency.agency_id, "Agency is without timezone"));
                continue;
            }
            ZoneId tz;
            try {
                tz = ZoneId.of(agency.agency_timezone);
            } catch (ZoneRulesException z) {
                feed.errors.add(new TimeZoneError("agency", agency.sourceFileLine, "agency_timezone", agency.agency_id, "Agency timezone wasn't found in timezone database reason: " + z.getMessage()));
                continue;
            } catch (DateTimeException dt) {
                feed.errors.add(new TimeZoneError("agency", agency.sourceFileLine, "agency_timezone", agency.agency_id, "Agency timezone in wrong format. Expected format: area/city"));
                //timezone will be set to GMT if it is still empty after for loop
                continue;
            }
        }
        for (Stop stop : feed.stops.values()) {
            index++;
            // stop_timezone is optional field. if null, continue.
            if (stop.stop_timezone == null) {
                continue;
            }
            ZoneId tz;
            try {
                tz = ZoneId.of(stop.stop_timezone);
            } catch (ZoneRulesException z) {
                feed.errors.add(new TimeZoneError("stops", stop.sourceFileLine, "stop_timezone", stop.stop_id, "Stop timezone wasn't found in timezone database reason: " + z.getMessage()));
                continue;
            } catch (DateTimeException dt) {
                feed.errors.add(new TimeZoneError("stops", stop.sourceFileLine, "stop_timezone", stop.stop_id, "Stop timezone in wrong format. Expected format: area/city"));
                //timezone will be set to GMT if it is still empty after for loop
                continue;
            }
        }
        return false;
    }
}
