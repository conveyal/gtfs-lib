package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.*;

import static com.conveyal.gtfs.error.NewGTFSErrorType.FIELD_VALUE_TOO_LONG;

/**
 * MTCValidator validates a GTFS feed according to the following
 * additional guidelines by 511 MTC:
 * - Field values should not exceed some number of characters.
 * Those requirements can be found by searching for the word 'character'.
 */
public class MTCValidator extends FeedValidator {

    public MTCValidator(Feed feed, SQLErrorStorage errorStorage) {
        super(feed, errorStorage);
    }

    @Override
    public void validate() {
        for (Agency agency : feed.agencies) {
            fieldLengthShouldNotExceed(agency, agency.agency_id, 50);
            fieldLengthShouldNotExceed(agency, agency.agency_name, 50);
            fieldLengthShouldNotExceed(agency, agency.agency_url, 500);
        }

        for (Stop stop : feed.stops) {
            fieldLengthShouldNotExceed(stop, stop.stop_name, 100);
        }

        for (Trip trip : feed.trips) {
            fieldLengthShouldNotExceed(trip, trip.trip_headsign, 120);
            fieldLengthShouldNotExceed(trip, trip.trip_short_name, 50);
        }

        // TODO: Handle calendar_attributes.txt?
    }

    boolean fieldLengthShouldNotExceed(Entity entity, Object objValue, int maxLength) {
        String value = objValue != null ? objValue.toString() : "";
        if (value.length() > maxLength) {
            if (errorStorage != null) registerError(entity, FIELD_VALUE_TOO_LONG, value);
            return false;
        }
        return true;
    }
}
