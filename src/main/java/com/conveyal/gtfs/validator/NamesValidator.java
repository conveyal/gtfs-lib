package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.Route;

import static com.conveyal.gtfs.error.NewGTFSErrorType.*;

public class NamesValidator extends FeedValidator {

    public NamesValidator(Feed feed, SQLErrorStorage errorStorage) {
        super(feed, errorStorage);
    }

    @Override
    public void validate() {
        // Check routes
        for (Route route : feed.routes) {
            String shortName = normalize(route.route_short_name);
            String longName = normalize(route.route_long_name);
            String desc = normalize(route.route_desc);
            // At least one of route_long_name and route_short_name must be supplied.
            // According to the GTFS spec these fields are required, but the logic is more complicated than for other fields.
            if (longName.isEmpty() && shortName.isEmpty()) {
                registerError(ROUTE_SHORT_AND_LONG_NAME_MISSING, null, route);
            }
            // Route_short_name should be really short, so it fits in a compact display e.g. on a mobile device.
            if (shortName.length() > 6) {
                registerError(ROUTE_SHORT_NAME_TOO_LONG, shortName, route);
            }
            // The long name should not contain the short name, it should contain different information.
            if (!longName.isEmpty() && !shortName.isEmpty() && longName.contains(shortName)) {
                registerError(ROUTE_LONG_NAME_CONTAINS_SHORT_NAME, longName, route);
            }
            // If provided, the description of a route should be more informative than its names.
            if (!desc.isEmpty() && (desc.equals(shortName) || desc.equals(longName))) {
                registerError(ROUTE_DESCRIPTION_SAME_AS_NAME, desc, route);
            }
            // Special range check for route_type.
            if (route.route_type < 0 || route.route_type > 7){
                // TODO we want some additional checking for extended route types.
            }
        }
        // TODO Check trips and all other tables.
    }

    /** @return a non-null String that is lower case and has no leading or trailing whitespace */
    private String normalize (String string) {
        if (string == null) return "";
        return string.trim().toLowerCase();
    }

}
