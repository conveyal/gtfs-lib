package com.conveyal.gtfs.loader;

/**
 * These are the conditionally required checks to be carried out inline with the values provided in
 * {@link ConditionallyRequiredField}.
 */
public enum ConditionallyRequiredFieldCheck {
    FIELD_NOT_EMPTY,
    FIELD_IN_RANGE
}