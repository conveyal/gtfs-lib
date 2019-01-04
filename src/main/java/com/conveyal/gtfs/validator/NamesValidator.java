package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.Trip;

import java.util.HashMap;
import java.util.Map;

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
                registerError(route, ROUTE_SHORT_AND_LONG_NAME_MISSING);
            }
            // Route_short_name should be really short, so it fits in a compact display e.g. on a mobile device.
            if (shortName.length() > 6) {
                registerError(route, ROUTE_SHORT_NAME_TOO_LONG, shortName);
            }
            // The long name should not contain the short name, it should contain different information.
            if (!longName.isEmpty() && !shortName.isEmpty() && longName.contains(shortName)) {
                registerError(route, ROUTE_LONG_NAME_CONTAINS_SHORT_NAME, longName);
            }
            // If provided, the description of a route should be more informative than its names.
            if (!desc.isEmpty() && (desc.equals(shortName) || desc.equals(longName))) {
                registerError(route, ROUTE_DESCRIPTION_SAME_AS_NAME, desc);
            }
            // Special range check for route_type.
            if (route.route_type < 0 || route.route_type > 7){
                // TODO we want some additional checking for extended route types.
            }
        }
        // Check stops
        for (Stop stop : feed.stops) {
            String name = normalize(stop.stop_name);
            String desc = normalize(stop.stop_desc);
            // Stops must be named.
            if (name.isEmpty()) {
                registerError(stop, STOP_NAME_MISSING);
            }
            // If provided, the description of a stop should be more informative than its name.
            if (!desc.isEmpty() && desc.equals(name)) {
                registerError(stop, STOP_DESCRIPTION_SAME_AS_NAME, desc);
            }
        }
        // Place routes into a map for quick access while validating trip names.
        Map<String, Route> routesForId = new HashMap<>();
        for (Route route : feed.routes) {
            routesForId.put(route.route_id, route);
        }
        // Check trip names (headsigns and TODO short names)
        for (Trip trip : feed.trips) {
            String headsign = normalize(trip.trip_headsign);
            // Trip headsign should not begin with "to" or "towards" (note: headsign normalized to lowercase). Headsigns
            // should follow one of the patterns defined in the best practices: http://gtfs.org/best-practices#tripstxt
            if (headsign.startsWith("to ") || headsign.startsWith("towards ")) {
                registerError(trip, TRIP_HEADSIGN_SHOULD_DESCRIBE_DESTINATION_OR_WAYPOINTS, headsign);
            }
            // TODO: check trip short name?
//            String shortName = normalize(trip.trip_short_name);
            Route route = routesForId.get(trip.route_id);
            // Skip route name/headsign check if the trip has a bad reference to its route.
            if (route == null) continue;
            String routeShortName = normalize(route.route_short_name);
            String routeLongName = normalize(route.route_long_name);
            // Trip headsign should not duplicate route name.
            if (!headsign.isEmpty() && (headsign.contains(routeShortName) || headsign.contains(routeLongName))) {
                registerError(trip, TRIP_HEADSIGN_CONTAINS_ROUTE_NAME, headsign);
            }
        }
        // TODO Are there other tables we're not checking?
    }

    /** @return a non-null String that is lower case and has no leading or trailing whitespace */
    private String normalize (String string) {
        if (string == null) return "";
        return string.trim().toLowerCase();
    }

}
