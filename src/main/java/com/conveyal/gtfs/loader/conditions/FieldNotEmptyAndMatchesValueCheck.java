package com.conveyal.gtfs.loader.conditions;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.loader.Field;
import com.conveyal.gtfs.loader.LineContext;
import com.google.common.collect.HashMultimap;

import java.util.HashSet;
import java.util.Set;

import static com.conveyal.gtfs.error.NewGTFSErrorType.CONDITIONALLY_REQUIRED;
import static com.conveyal.gtfs.loader.JdbcGtfsLoader.POSTGRES_NULL_TEXT;

/**
 * Conditional requirement to check that a dependent field value is not empty and matches an expected value.
 */
public class FieldNotEmptyAndMatchesValueCheck extends ConditionalRequirement {
    /** The expected dependent field value. */
    private String requiredDependentFieldValue;

    public FieldNotEmptyAndMatchesValueCheck(
        String dependentFieldName,
        String requiredDependentFieldValue
    ) {
        this.dependentFieldName = dependentFieldName;
        this.requiredDependentFieldValue = requiredDependentFieldValue;
    }

    /**
     * Check the dependent field value is not empty and matches the expected value.
     */
    public Set<NewGTFSError> check(
        LineContext lineContext,
        Field referenceField,
        HashMultimap<String, String> uniqueValuesForFields
    ) {
        Set<NewGTFSError> errors = new HashSet<>();
        String dependentFieldValue = lineContext.getValueForRow(dependentFieldName);
        String referenceFieldValue = lineContext.getValueForRow(referenceField.name);
        if (
            !POSTGRES_NULL_TEXT.equals(dependentFieldValue) &&
                dependentFieldValue.equals(requiredDependentFieldValue) &&
                POSTGRES_NULL_TEXT.equals(referenceFieldValue)
        ) {
            String message = String.format(
                "%s is required and must match %s when %s is provided.",
                referenceField.name,
                requiredDependentFieldValue,
                dependentFieldName
            );
            errors.add(
                NewGTFSError.forLine(
                    lineContext,
                    CONDITIONALLY_REQUIRED,
                    message).setEntityId(lineContext.getEntityId())
            );

        }
        return errors;
    }

}
