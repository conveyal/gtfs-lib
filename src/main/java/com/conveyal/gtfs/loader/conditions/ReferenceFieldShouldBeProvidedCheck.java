package com.conveyal.gtfs.loader.conditions;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.loader.Field;
import com.conveyal.gtfs.loader.LineContext;
import com.google.common.collect.HashMultimap;

import java.util.HashSet;
import java.util.Set;

import static com.conveyal.gtfs.error.NewGTFSErrorType.AGENCY_ID_REQUIRED_FOR_MULTI_AGENCY_FEEDS;
import static com.conveyal.gtfs.loader.JdbcGtfsLoader.POSTGRES_NULL_TEXT;
import static com.conveyal.gtfs.loader.conditions.ConditionalCheckType.HAS_MULTIPLE_ROWS;

/**
 * Conditional requirement to check that the reference field is not empty when the dependent field/table has multiple
 * rows.
 */
public class ReferenceFieldShouldBeProvidedCheck extends ConditionalRequirement {

    public ReferenceFieldShouldBeProvidedCheck(
        String dependentFieldName,
        ConditionalCheckType dependentFieldCheck,
        ConditionalCheckType referenceFieldCheck
    ) {
        this.dependentFieldName = dependentFieldName;
        this.dependentFieldCheck = dependentFieldCheck;
        this.referenceFieldCheck = referenceFieldCheck;
    }

    /**
     * Checks that the reference field is not empty when the dependent field/table has multiple rows. This is
     * principally designed for checking that routes#agency_id is filled when multiple agencies exist.
     */
    public Set<NewGTFSError> check(
        LineContext lineContext,
        Field referenceField,
        HashMultimap<String, String> uniqueValuesForFields
    ) {
        String referenceFieldValue = lineContext.getValueForRow(referenceField.name);
        Set<NewGTFSError> errors = new HashSet<>();
        int dependentFieldCount = uniqueValuesForFields.get(dependentFieldName).size();
        if (dependentFieldCheck == HAS_MULTIPLE_ROWS && dependentFieldCount > 1) {
            // If there are multiple entries for the dependent field (including empty strings to account for any
            // potentially missing values), the reference field must not be empty.
            boolean referenceFieldIsEmpty = POSTGRES_NULL_TEXT.equals(referenceFieldValue);
            if (referenceFieldIsEmpty) {
                errors.add(
                    NewGTFSError.forLine(
                        lineContext,
                        AGENCY_ID_REQUIRED_FOR_MULTI_AGENCY_FEEDS,
                        null
                    ).setEntityId(lineContext.getEntityId())
                );
            }
        }
        return errors;
    }

}
