package com.conveyal.gtfs.loader;

/**
 * These are the conditionally required checks to be carried out inline with the values provided in
 * {@link ConditionalRequirement}.
 */
public enum ConditionalCheckType {
    /**
     * The conditionally required field value must not be empty. This is used in conjunction with
     * {@link ConditionalCheckType#FIELD_IN_RANGE}. E.g. if the reference field is within a specified range, the
     * conditionally required field must not be empty.
     */
    FIELD_NOT_EMPTY,
    /**
     * The reference field value must be within a specified range.
     */
    FIELD_IN_RANGE,
    /**
     * The reference field value must be available in order to match the conditionally required field value.
     */
    FOREIGN_FIELD_VALUE_MATCH,
    /**
     * If the reference table row count is greater than one, the conditionally required field values must not be empty.
     * This is used in conjunction with {@link ConditionalCheckType#FIELD_NOT_EMPTY}.
     */
    ROW_COUNT_GREATER_THAN_ONE
}