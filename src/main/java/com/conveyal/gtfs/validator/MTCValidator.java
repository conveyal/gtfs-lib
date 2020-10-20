package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.*;

import java.net.URL;

import static com.conveyal.gtfs.error.NewGTFSErrorType.FIELD_VALUE_TOO_LONG;
import static com.conveyal.gtfs.error.NewGTFSErrorType.SERVICE_WITHOUT_DAYS;

/**
 * MTCValidator runs a set of custom validation checks for GTFS feeds managed by MTC in Data Tools.
 * At this time, the checks consist of validating field lengths and that calendars apply to
 * at least one day of the week per the 511 MTC guidelines at
 * https://github.com/ibi-group/datatools-ui/files/4438625/511.Transit_Data.Guidelines_V2.0_3-27-2020.pdf.
 * For specific field lengths, search the guidelines for the word 'character'.
 *
 * Note that other validations, e.g. on GTFS+ files, are discussed in
 * https://github.com/ibi-group/datatools-ui/issues/544.
 */
public class MTCValidator extends FeedValidator {

    public MTCValidator(Feed feed, SQLErrorStorage errorStorage) {
        super(feed, errorStorage);
    }

    @Override
    public void validate() {
        // Validate field lengths (agency, stop, trip).
        for (Agency agency : feed.agencies) {
            validateFieldLength(agency, agency.agency_id, 50);
            validateFieldLength(agency, agency.agency_name, 50);
            validateFieldLength(agency, agency.agency_url, 500);
        }

        for (Stop stop : feed.stops) {
            validateFieldLength(stop, stop.stop_name, 100);
        }

        for (Trip trip : feed.trips) {
            validateFieldLength(trip, trip.trip_headsign, 120);
            validateFieldLength(trip, trip.trip_short_name, 50);
        }

        // Validate that calendars apply to at least one day of the week.
        for (Calendar calendar : feed.calendars) {
            validateCalendarDays(calendar);
        }
    }

    /**
     * Checks that a {@link Calendar} entity is applicable for at least one day of the week
     * (i.e. at least one of the fields for Monday-Sunday is set to '1').
     * @param calendar The {@link Calendar} entity to check.
     * @return true if at least one field for Monday-Sunday is set to 1, false otherwise.
     */
    public boolean validateCalendarDays(Calendar calendar) {
        boolean result = calendar.monday == 1
            || calendar.tuesday == 1
            || calendar.wednesday == 1
            || calendar.thursday == 1
            || calendar.friday == 1
            || calendar.saturday == 1
            || calendar.sunday == 1;

        if (!result) {
            if (errorStorage != null) registerError(calendar, SERVICE_WITHOUT_DAYS, "[Service applies to no day of the week.]");
        }
        return result;
    }

    /**
     * Checks that the length of a string does not exceed a certain length.
     * Reports an error if the length is exceeded.
     * @param entity The containing GTFS entity (for error reporting purposes).
     * @param value The String value to check.
     * @param maxLength The length to check, should be positive or zero.
     * @return true if value.length() is maxLength or less, or if value is null; false otherwise.
     */
    public boolean validateFieldLength(Entity entity, String value, int maxLength) {
        if (value != null && value.length() > maxLength) {
            if (errorStorage != null) registerError(entity, FIELD_VALUE_TOO_LONG, "[over " + maxLength + " characters] " + value);
            return false;
        }
        return true;
    }

    public boolean validateFieldLength(Entity entity, URL url, int maxLength) {
        return validateFieldLength(entity, url != null ? url.toString() : "", maxLength);
    }
}
