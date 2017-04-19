package com.conveyal.gtfs.error;

import com.conveyal.gtfs.validator.model.Priority;

/**
 * TODO remove entity type prefixes where not needed
 */
public enum NewGTFSErrorType {

    ROUTE_DESCRIPTION_SAME_AS_NAME (Priority.MEDIUM),
    ROUTE_LONG_NAME_CONTAINS_SHORT_NAME (Priority.MEDIUM),
    ROUTE_SHORT_AND_LONG_NAME_MISSING (Priority.MEDIUM),
    ROUTE_SHORT_NAME_TOO_LONG (Priority.MEDIUM),
    LOW_POPULATION_DENSITY(Priority.MEDIUM),
    GEOGRAPHIC_OUTLIER(Priority.MEDIUM),
    STOP_NOT_REFERENCED_BY_STOP_TIMES (Priority.MEDIUM),
    TRIP_NOT_REFERENCED_BY_STOP_TIMES (Priority.MEDIUM),
    ROUTE_NOT_REFERENCED_BY_STOP_TIMES (Priority.MEDIUM),
    TRAVEL_DISTANCE_ZERO(Priority.MEDIUM),
    TRAVEL_TIME_NEGATIVE(Priority.MEDIUM),
    TRAVEL_TIME_ZERO(Priority.MEDIUM),
    MISSING_ARRIVAL_OR_DEPARTURE(Priority.MEDIUM),
    TRIP_TOO_FEW_STOP_TIMES (Priority.MEDIUM),
    TRAVEL_TOO_SLOW(Priority.LOW),
    TRAVEL_TOO_FAST(Priority.MEDIUM),
    DEPARTURE_BEFORE_ARRIVAL(Priority.HIGH),
    NO_TIME_ZONE(Priority.HIGH),
    BAD_TIME_ZONE(Priority.MEDIUM),
    DUPLICATE_STOP(Priority.LOW);

    final Priority priority;

    NewGTFSErrorType(Priority priority) {
        this.priority = priority;
    }

}


