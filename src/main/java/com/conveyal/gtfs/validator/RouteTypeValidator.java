package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.Route;

import java.util.List;

import static com.conveyal.gtfs.error.NewGTFSErrorType.ROUTE_TYPE_INVALID;

/**
 * Validates route types based on a configured list of values.
 */
public class RouteTypeValidator extends FeedValidator {
    private final List<Integer> configuredRouteTypes;

    /**
     * Constructor used for tests in the same package.
     */
    RouteTypeValidator(List<Integer> routeTypes) {
        this(null, null, routeTypes);
    }

    /**
     * Constructor for building a route type validator given a set of valid route types.
     */
    public RouteTypeValidator(Feed feed, SQLErrorStorage errorStorage, List<Integer> routeTypes) {
        super(feed, errorStorage);
        this.configuredRouteTypes = routeTypes;
    }

    @Override
    public void validate() {
        feed.routes.forEach(this::validateRouteType);
    }

    /**
     * @return true if routeType is one of the configured route types, false otherwise.
     */
    public boolean isRouteTypeValid(int routeType) {
        return configuredRouteTypes.contains(routeType);
    }

    /**
     * Checks that the route type is valid, reports a validation error if not.
     * @param route The containing GTFS route.
     * @return true if the route type for the given route is valid, false otherwise.
     */
    public boolean validateRouteType(Route route) {
        if (!isRouteTypeValid(route.route_type)) {
            if (errorStorage != null) registerError(route, ROUTE_TYPE_INVALID, route.route_type);
            return false;
        }

        return true;
    }
}
