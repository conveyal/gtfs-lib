package com.conveyal.gtfs.loader;

/**
 * These are the conditionally required checks to be carried out inline with the values provided in
 * {@link ConditionalRequirement}.
 */
public enum ConditionalCheckType {
    /**
     * The conditionally required field value must not be empty. This is used in conjunction with
     * {@link ConditionalCheckType#FIELD_IN_RANGE}. E.g. if the reference field is within a specified range, the
     * dependent field must not be empty.
     */
    FIELD_NOT_EMPTY,
    /**
     * The reference field value must be within a specified range.
     */
    FIELD_IN_RANGE,
    /**
     * This checks that the foreign reference exists in the dependent field (e.g., stops#zone_id).
     */
    FOREIGN_REF_EXISTS,
    /**
     * Check that the reference table has multiple records. This is sometimes used in conjunction with
     * {@link ConditionalCheckType#FIELD_NOT_EMPTY} (e.g., to check that multiple agencies exist).
     */
    HAS_MULTIPLE_ROWS
}