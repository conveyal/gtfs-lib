package com.conveyal.gtfs.loader;

public enum ConditionallyRequiredForeignRefCheck {
    STOPS_ZONE_ID_FARE_RULES_FOREIGN_REF_CHECK // Confirm that all zone_id references in fare rules are available in stops.
}
