package com.conveyal.gtfs.loader.conditions;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.loader.Field;
import com.conveyal.gtfs.loader.LineContext;
import com.google.common.collect.HashMultimap;

import java.util.HashSet;
import java.util.Set;

import static com.conveyal.gtfs.error.NewGTFSErrorType.CONDITIONALLY_REQUIRED;
import static com.conveyal.gtfs.loader.JdbcGtfsLoader.POSTGRES_NULL_TEXT;
import static com.conveyal.gtfs.loader.conditions.ConditionalCheckType.FIELD_NOT_EMPTY;

/**
 * Conditional requirement to check that a reference field value is within a defined range and the conditional field
 * value has not be defined.
 */
public class FieldInRangeCheck extends ConditionalRequirement {

    public FieldInRangeCheck(
        int minReferenceValue,
        int maxReferenceValue,
        String dependentFieldName,
        ConditionalCheckType dependentFieldCheck,
        ConditionalCheckType referenceFieldCheck
    ) {
        this.minReferenceValue = minReferenceValue;
        this.maxReferenceValue = maxReferenceValue;
        this.dependentFieldName = dependentFieldName;
        this.dependentFieldCheck = dependentFieldCheck;
        this.referenceFieldCheck = referenceFieldCheck;
    }

    /**
     * If the reference field value is within a defined range and the conditional field value has not be defined, flag
     * an error.
     */
    public Set<NewGTFSError> check(
        LineContext lineContext,
        Field referenceField,
        HashMultimap<String, String> uniqueValuesForFields
    ) {
        Set<NewGTFSError> errors = new HashSet<>();
        String referenceFieldValue = lineContext.getValueForRow(referenceField.name);
        String conditionalFieldValue = lineContext.getValueForRow(dependentFieldName);
        boolean referenceValueMeetsRangeCondition =
            !POSTGRES_NULL_TEXT.equals(referenceFieldValue) &&
                // TODO use pre-existing method in ShortField?
                isValueInRange(referenceFieldValue, minReferenceValue, maxReferenceValue);

        if (!referenceValueMeetsRangeCondition) {
            // If ref value does not meet the range condition, there is no need to check this conditional
            // value for (e.g.) an empty value. Continue to the next check.
            return errors;
        }
        boolean conditionallyRequiredValueIsEmpty =
            dependentFieldCheck == FIELD_NOT_EMPTY &&
                POSTGRES_NULL_TEXT.equals(conditionalFieldValue);

        if (conditionallyRequiredValueIsEmpty) {
            // Reference value in range and conditionally required field is empty.
            String message = String.format(
                "%s is required when %s value is between %d and %d.",
                dependentFieldName,
                referenceField.name,
                minReferenceValue,
                maxReferenceValue
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

    /**
     * Check if the provided value is within the min and max values. If the field value can not be converted
     * to a number it is assumed that the value is not a number and will therefore never be within the min/max range.
     */
    private boolean isValueInRange(String referenceFieldValue, int min, int max) {
        try {
            int fieldValue = Integer.parseInt(referenceFieldValue);
            return fieldValue >= min && fieldValue <= max;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
