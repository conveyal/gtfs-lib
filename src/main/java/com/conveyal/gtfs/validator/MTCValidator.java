package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.*;

import static com.conveyal.gtfs.error.NewGTFSErrorType.FIELD_VALUE_TOO_LONG;

/**
 * MTCValidator checks in a GTFS feed that the length of certain field values
 * do not exceed the 511 MTC guidelines. (TODO: add guidelines URL.)
 * To refer to specific limits, search the guidelines for the word 'character'.
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

    /**
     * Checks that the length of a string (or Object.toString()) does not exceed a length.
     * Reports an error if the length is exceeded.
     * @param entity The containing GTFS entity (for error reporting purposes).
     * @param objValue The value to check.
     * @param maxLength The length to check, should be positive or zero.
     * @return true if the length of objValue.toString() is maxLength or less or if objValue is null; false otherwise.
     */
    public boolean fieldLengthShouldNotExceed(Entity entity, Object objValue, int maxLength) {
        String value = objValue != null ? objValue.toString() : "";
        if (value.length() > maxLength) {
            if (errorStorage != null) registerError(entity, FIELD_VALUE_TOO_LONG, "[over " + maxLength + " characters] " + value);
            return false;
        }
        return true;
    }
}
