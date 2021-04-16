package com.conveyal.gtfs.loader;

/**
 * These are the conditionally required checks to be carried out inline with the values provided in
 * {@link ConditionallyRequired}.
 */
public enum ConditionallyRequiredCheck {
    LOCATION_TYPE_STOP_NAME_CHECK,
    LOCATION_TYPE_STOP_LAT_CHECK,
    LOCATION_TYPE_STOP_LON_CHECK,
    LOCATION_TYPE_PARENT_STATION_CHECK,
}
