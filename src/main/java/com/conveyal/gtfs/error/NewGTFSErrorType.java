package com.conveyal.gtfs.error;

import com.conveyal.gtfs.validator.model.Priority;

/**
 * TODO remove entity type prefixes where not needed
 */
public enum NewGTFSErrorType {

    DATE_FORMAT, // Format should be YYYYMMDD
    TIME_FORMAT, // Format should be HH:MM:SS
    URL_FORMAT, // Format should be <scheme>://<authority><path>?<query>#<fragment>
    INTEGER_FORMAT,
    FLOATING_FORMAT,
    TABLE_NAME_FORMAT,
    NUMBER_NEGATIVE,
    NUMBER_RANGE, // Number %s outside of acceptable range
    DUPLICATE_ID,
    DUPLICATE_TRIP,
    DUPLICATE_STOP,
    DUPLICATE_HEADER,
    MISSING_TABLE, // This table is required by the GTFS specification but is missing
    MISSING_COLUMN,
    MISSING_SHAPE,
    MISSING_FIELD,
    WRONG_NUMBER_OF_FIELDS,
    OVERLAPPING_TRIP,
    SHAPE_REVERSED,
    SHAPE_MISSING_COORDINATE,
    TABLE_IN_SUBDIRECTORY, // All GTFS files (including %s.txt) should be at root of zipfile, not nested in subdirectory
    TABLE_TOO_LONG, // Table is too long to record line numbers with an integer, overflow will occur.
    TIME_ZONE_FORMAT,
    ROUTE_DESCRIPTION_SAME_AS_NAME,
    ROUTE_LONG_NAME_CONTAINS_SHORT_NAME,
    ROUTE_SHORT_AND_LONG_NAME_MISSING,
    ROUTE_SHORT_NAME_TOO_LONG,
    STOP_LOW_POPULATION_DENSITY,
    STOP_GEOGRAPHIC_OUTLIER,
    STOP_UNUSED,
    TRIP_EMPTY,
    ROUTE_UNUSED,
    TRAVEL_DISTANCE_ZERO,
    TRAVEL_TIME_NEGATIVE,
    TRAVEL_TIME_ZERO,
    MISSING_ARRIVAL_OR_DEPARTURE,
    TRIP_TOO_FEW_STOP_TIMES,
    TRIP_OVERLAP_IN_BLOCK,
    TRAVEL_TOO_SLOW,
    TRAVEL_TOO_FAST,
    DEPARTURE_BEFORE_ARRIVAL,
    OTHER;

    final Priority priority;

    NewGTFSErrorType() {
        this.priority = Priority.MEDIUM;
    }
    NewGTFSErrorType(Priority priority) {
        this.priority = priority;
    }

}


