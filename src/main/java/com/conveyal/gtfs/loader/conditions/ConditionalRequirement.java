package com.conveyal.gtfs.loader.conditions;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.loader.Field;
import com.conveyal.gtfs.loader.LineContext;
import com.google.common.collect.HashMultimap;

import java.util.Set;

/**
 * An abstract class which primarily defines a method used by implementing classes to define specific conditional
 * requirement checks.
 */
public abstract class ConditionalRequirement {
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
