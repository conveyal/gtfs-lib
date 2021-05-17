package com.conveyal.gtfs.loader.conditions;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.loader.Field;
import com.conveyal.gtfs.loader.LineContext;
import com.google.common.collect.HashMultimap;

import java.util.Set;

/**
 * These are the requirements that are checked inline with {@link ConditionalCheckType} to determine if the required
 * conditions set forth for certain fields in the GTFS spec have been met. These requirements are applied directly to
 * their "reference fields" with the help of {@link Field#requireConditions}.
 */
public abstract class ConditionalRequirement {
    /** The type of check to be performed on a reference field. A reference field value is used to determine which check
     * (e.g., {@link AgencyHasMultipleRowsCheck#check}) should be applied to the field. */
    protected ConditionalCheckType referenceFieldCheck;
    /** The minimum reference field value if a range check is being performed. */
    protected int minReferenceValue;
    /** The maximum reference field value if a range check is being performed. */
    protected int maxReferenceValue;
    /** The type of check to be performed on the dependent field. */
    protected ConditionalCheckType dependentFieldCheck;
    /** The name of the dependent field, which is a field that requires a specific value if the reference and
     * (in some cases) dependent field checks meet certain conditions.*/
    protected String dependentFieldName;
    /** The expected dependent field value. */
    protected String dependentFieldValue;
    /** The reference table name required for checking foreign references. */
    protected String referenceTableName;

    /**
     * All sub classes must implement this method and provide related conditional checks.
     */
    public abstract Set<NewGTFSError> check(
        LineContext lineContext,
        Field referenceField,
        HashMultimap<String, String> uniqueValuesForFields
    );
}
