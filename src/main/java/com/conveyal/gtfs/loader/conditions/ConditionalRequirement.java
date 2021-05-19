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
    /** The type of check to be performed on the dependent field. */
    protected ConditionalCheckType dependentFieldCheck;
    /** The name of the dependent field, which is a field that requires a specific value if the reference and
     * (in some cases) dependent field checks meet certain conditions.*/
    protected String dependentFieldName;

    /**
     * All sub classes must implement this method and provide related conditional checks.
     */
    public abstract Set<NewGTFSError> check(
        LineContext lineContext,
        Field referenceField,
        HashMultimap<String, String> uniqueValuesForFields
    );
}
