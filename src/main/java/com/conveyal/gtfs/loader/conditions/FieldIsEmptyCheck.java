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
 * Conditional requirement to check that if a dependent field value is empty then the reference field value is provided.
 */
public class FieldIsEmptyCheck extends ConditionalRequirement {

    public FieldIsEmptyCheck(String dependentFieldName) {
        this.dependentFieldName = dependentFieldName;
    }

    /**
     * Check the dependent field value. If it is empty, the reference field value must be provided.
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
            POSTGRES_NULL_TEXT.equals(dependentFieldValue) &&
            POSTGRES_NULL_TEXT.equals(referenceFieldValue)
        ) {
            // The reference field is required when the dependent field is empty.
            String message = String.format(
                "%s is required when %s is empty.",
                referenceField.name,
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
