package com.conveyal.gtfs.loader;

/**
 * These are the conditionally required checks to be carried out inline with the values provided in
 * {@link ConditionalRequirement}.
 */
public enum ConditionalCheckType {
    FIELD_NOT_EMPTY,
    FIELD_IN_RANGE,
    FOREIGN_FIELD_VALUE_MATCH,
    ROW_COUNT_GREATER_THAN_ONE
}