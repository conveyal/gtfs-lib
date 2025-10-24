package com.conveyal.gtfs.model;

/**
 * Utility class for mapping GTFS route types between standard and extended types.
 *
 * Standard route types are 0-7 as defined in the GTFS specification.
 * Extended route types (100-1799) provide more specificity but map back to standard types.
 *
 * See: https://developers.google.com/transit/gtfs/reference/extended-route-types
 */
public final class RouteTypeUtil {

    private RouteTypeUtil() {}

    /**
     * Maps a route type (standard or extended) to its corresponding standard route type (0-7).
     *
     * If the route type is already a standard type (0-7), it is returned unchanged.
     *
     * @param routeType The route type code to map
     * @return The standard route type (0-7) corresponding to the input
     */
    public static int getStandardRouteType(int routeType) {
        // If already a standard type (0-7), return it unchanged
        if (routeType >= 0 && routeType <= 7) {
            return routeType;
        }

        ExtendedRouteType extended = ExtendedRouteType.fromCode(routeType);
        if(extended != null) {
            if(extended.getStandardRouteType().isPresent()) {
                return extended.getStandardRouteType().get().getCode();
            } else {
                throw new RuntimeException("Extended route type " + routeType + " has no standard route type.");
            }
        } else {
            throw new RuntimeException("Route type " + routeType + " is neither an extended nor a standard route type.");
        }
    }

    /**
     * Returns true if the given route type is an extended type (100+).
     *
     * @param routeType The route type code to check
     * @return true if the route type is extended (>= 100), false otherwise
     */
    public static boolean isExtendedRouteType(int routeType) {
        return routeType >= 100;
    }
}