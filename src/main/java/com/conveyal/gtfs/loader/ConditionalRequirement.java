package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.error.NewGTFSError;
import com.google.common.collect.TreeMultimap;

import java.util.HashSet;
import java.util.NavigableSet;
import java.util.Set;

import static com.conveyal.gtfs.error.NewGTFSErrorType.AGENCY_ID_REQUIRED_FOR_MULTI_AGENCY_FEEDS;
import static com.conveyal.gtfs.error.NewGTFSErrorType.CONDITIONALLY_REQUIRED;
import static com.conveyal.gtfs.error.NewGTFSErrorType.REFERENTIAL_INTEGRITY;
import static com.conveyal.gtfs.loader.ConditionalCheckType.FIELD_NOT_EMPTY;
import static com.conveyal.gtfs.loader.ConditionalCheckType.HAS_MULTIPLE_ROWS;
import static com.conveyal.gtfs.loader.JdbcGtfsLoader.POSTGRES_NULL_TEXT;

/**
 * These are the requirements that are checked inline with {@link ConditionalCheckType} to determine if the required
 * conditions set forth for certain fields in the GTFS spec have been met. These requirements are applied directly to
 * their "reference fields" with the help of {@link Field#requireConditions}.
 */
public class ConditionalRequirement {
    private static final int FIRST_ROW = 2;
    private static final int SECOND_ROW = 3;
    /** The type of check to be performed on a reference field. A reference field value is used to determine which check
     * (e.g., {@link #checkHasMultipleRows}) should be applied to the field. */
    public ConditionalCheckType referenceFieldCheck;
    /** The minimum reference field value if a range check is being performed. */
    public int minReferenceValue;
    /** The maximum reference field value if a range check is being performed. */
    public int maxReferenceValue;
    /** The type of check to be performed on the dependent field. */
    public ConditionalCheckType dependentFieldCheck;
    /** The name of the dependent field, which is a field that requires a specific value if the reference and
     * (in some cases) dependent field checks meet certain conditions.*/
    public String dependentFieldName;

    public ConditionalRequirement(
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

    public ConditionalRequirement(
        String dependentFieldName,
        ConditionalCheckType referenceFieldCheck
    ) {
        this(0,0, dependentFieldName, null, referenceFieldCheck);
    }

    public ConditionalRequirement(
        String dependentFieldName,
        ConditionalCheckType dependentFieldCheck,
        ConditionalCheckType referenceFieldCheck
    ) {
        this(0,0, dependentFieldName, dependentFieldCheck, referenceFieldCheck);
    }

    /**
     * Flag an error if there are multiple rows (designed for agency.txt) and the agency_id is missing for any rows.
     */
    public static Set<NewGTFSError> checkHasMultipleRows(
        LineContext lineContext,
        TreeMultimap<String, String> uniqueValuesForFields,
        ConditionalRequirement check
    ) {
        String dependentFieldValue = lineContext.getValueForRow(check.dependentFieldName);
        Set<NewGTFSError> errors = new HashSet<>();
        NavigableSet<String> agencyIdValues = uniqueValuesForFields.get(check.dependentFieldName);
        // Do some awkward checks to determine if the first or second row (or another) is missing the agency_id.
        boolean firstOrSecondMissingId = lineContext.lineNumber == SECOND_ROW && agencyIdValues.contains("");
        boolean currentRowMissingId = POSTGRES_NULL_TEXT.equals(dependentFieldValue);
        boolean secondRowMissingId = firstOrSecondMissingId && currentRowMissingId;
        if (firstOrSecondMissingId || (lineContext.lineNumber > SECOND_ROW && currentRowMissingId)) {
            // The check on the agency table is carried out whilst the agency table is being loaded so it
            // is possible to compare the number of transitIds added against the number of rows loaded to
            // accurately determine missing agency_id values.
            int lineNumber = secondRowMissingId
                ? SECOND_ROW
                : firstOrSecondMissingId
                    ? FIRST_ROW
                    : lineContext.lineNumber;
            errors.add(
                NewGTFSError.forLine(
                    lineContext.table,
                    lineNumber,
                    AGENCY_ID_REQUIRED_FOR_MULTI_AGENCY_FEEDS,
                    check.dependentFieldName
                )
            );
        }
        return errors;
    }

    /**
     * Checks that the reference field is not empty when the dependent field/table has multiple rows. This is
     * principally designed for checking that routes#agency_id is filled when multiple agencies exist.
     */
    public static Set<NewGTFSError> checkFieldEmpty(
        LineContext lineContext,
        Field referenceField,
        TreeMultimap<String, String> uniqueValuesForFields,
        ConditionalRequirement check
    ) {
        String referenceFieldValue = lineContext.getValueForRow(referenceField.name);
        Set<NewGTFSError> errors = new HashSet<>();
        int dependentFieldCount = uniqueValuesForFields.get(check.dependentFieldName).size();
        if (check.dependentFieldCheck == HAS_MULTIPLE_ROWS && dependentFieldCount > 1) {
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

    /**
     * If the reference field value is within a defined range and the conditional field value has not be defined, flag
     * an error.
     */
    public static Set<NewGTFSError> checkFieldInRange(
        LineContext lineContext,
        Field referenceField,
        ConditionalRequirement check
    ) {
        Set<NewGTFSError> errors = new HashSet<>();
        String referenceFieldValue = lineContext.getValueForRow(referenceField.name);
        String conditionalFieldValue = lineContext.getValueForRow(check.dependentFieldName);
        boolean referenceValueMeetsRangeCondition =
            !POSTGRES_NULL_TEXT.equals(referenceFieldValue) &&
                // TODO use pre-existing method in ShortField?
                isValueInRange(referenceFieldValue, check.minReferenceValue, check.maxReferenceValue);

        if (!referenceValueMeetsRangeCondition) {
            // If ref value does not meet the range condition, there is no need to check this conditional
            // value for (e.g.) an empty value. Continue to the next check.
            return errors;
        }
        boolean conditionallyRequiredValueIsEmpty =
            check.dependentFieldCheck == FIELD_NOT_EMPTY &&
                POSTGRES_NULL_TEXT.equals(conditionalFieldValue);

        if (conditionallyRequiredValueIsEmpty) {
            // Reference value in range and conditionally required field is empty.
            String message = String.format(
                "%s is conditionally required when %s value is between %d and %d.",
                check.dependentFieldName,
                referenceField.name,
                check.minReferenceValue,
                check.maxReferenceValue
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
     * Check that an expected foreign field value matches a conditional field value. Selected foreign field values are
     * added to {@link ReferenceTracker#uniqueValuesForFields} as part of the load process and are used here to check
     * conditional fields which have a dependency on them.
     */
    public static Set<NewGTFSError> checkForeignRefExists(
        LineContext lineContext,
        Field referenceField,
        ConditionalRequirement check,
        TreeMultimap<String, String> uniqueValuesForFields
    ) {
        Set<NewGTFSError> errors = new HashSet<>();
        String referenceFieldValue = lineContext.getValueForRow(referenceField.name);
        // Expected reference in foreign field id list.
        String foreignFieldReference =
            String.join(
                ":",
                check.dependentFieldName,
                referenceFieldValue
            );
        if (lineContext.table.name.equals("fare_rules") &&
            !POSTGRES_NULL_TEXT.equals(referenceFieldValue) &&
            !uniqueValuesForFields.get(check.dependentFieldName).contains(foreignFieldReference)
        ) {
            // stop#zone_id does not exist in stops table, but is required by fare_rules records (e.g., origin_id).
            errors.add(
                NewGTFSError.forLine(
                    lineContext,
                    REFERENTIAL_INTEGRITY,
                    String.join(":", referenceField.name, foreignFieldReference)
                ).setEntityId(lineContext.getEntityId())
            );
        }
        return errors;
    }

    /**
     * Check if the provided value is within the min and max values. If the field value can not be converted
     * to a number it is assumed that the value is not a number and will therefore never be within the min/max range.
     */
    private static boolean isValueInRange(String referenceFieldValue, int min, int max) {
        try {
            int fieldValue = Integer.parseInt(referenceFieldValue);
            return fieldValue >= min && fieldValue <= max;
        } catch (NumberFormatException e) {
            return false;
        }
    }

}
