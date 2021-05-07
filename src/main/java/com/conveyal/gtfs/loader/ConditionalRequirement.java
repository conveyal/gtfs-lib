package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.error.NewGTFSError;

import java.util.HashSet;
import java.util.Set;

import static com.conveyal.gtfs.error.NewGTFSErrorType.AGENCY_ID_REQUIRED_FOR_TABLES_WITH_MORE_THAN_ONE_RECORD;
import static com.conveyal.gtfs.error.NewGTFSErrorType.CONDITIONALLY_REQUIRED;
import static com.conveyal.gtfs.loader.ConditionalCheckType.FIELD_NOT_EMPTY;
import static com.conveyal.gtfs.loader.JdbcGtfsLoader.POSTGRES_NULL_TEXT;

/**
 * These are the values that are checked inline with {@link ConditionalCheckType} to determine if the required
 * conditions have been met.
 */
public class ConditionalRequirement {
    /** The type of check to be performed on a reference field. A reference field value is used to determine whether or
     * not a conditional field is required. */
    public ConditionalCheckType referenceCheck;
    /** The minimum reference field value if a range check is being performed. */
    public int minReferenceValue;
    /** The maximum reference field value if a range check is being performed. */
    public int maxReferenceValue;
    /** The type of check to be performed on a conditional field. A conditional field is one that may require a value
     * if the reference and conditional checks met certain conditions. */
    public ConditionalCheckType conditionalCheck;
    /** The name of the conditional field. */
    public String conditionalFieldName;
    /** The expected conditional field value. */
    public String conditionalFieldValue;

    public ConditionalRequirement(
        int minReferenceValue,
        int maxReferenceValue,
        String conditionalFieldName,
        String conditionalFieldValue,
        ConditionalCheckType conditionalCheck,
        ConditionalCheckType referenceCheck

    ) {
        this.minReferenceValue = minReferenceValue;
        this.maxReferenceValue = maxReferenceValue;
        this.conditionalFieldName = conditionalFieldName;
        this.conditionalFieldValue = conditionalFieldValue;
        this.conditionalCheck = conditionalCheck;
        this.referenceCheck = referenceCheck;
    }

    public ConditionalRequirement(
        int minReferenceValue,
        int maxReferenceValue,
        String conditionalFieldName,
        ConditionalCheckType conditionalCheck,
        ConditionalCheckType referenceCheck

    ) {
        this(minReferenceValue,maxReferenceValue, conditionalFieldName, null, conditionalCheck, referenceCheck);
    }

    public ConditionalRequirement(
        String conditionalFieldName,
        ConditionalCheckType referenceCheck
    ) {
        this(0,0, conditionalFieldName, null, null, referenceCheck);
    }

    public ConditionalRequirement(
        String conditionalFieldName,
        ConditionalCheckType conditionalCheck,
        ConditionalCheckType referenceCheck
    ) {
        this(0,0, conditionalFieldName, null,conditionalCheck, referenceCheck);
    }

    public ConditionalRequirement(
        String conditionalFieldName,
        String conditionalFieldValue,
        ConditionalCheckType referenceCheck
    ) {
        this(0,0, conditionalFieldName, conditionalFieldValue, null, referenceCheck);
    }


    /**
     * Flag an error if the number of rows in the agency table is greater than one and the agency_id has not been defined
     * for each row.
     */
    public static Set<NewGTFSError> checkRowCountGreaterThanOne(
        Table table,
        int lineNumber,
        Set<String> transitIds,
        ConditionalRequirement check,
        String conditionalFieldValue,
        String entityId
    ) {
        Set<NewGTFSError> errors = new HashSet<>();
        if (table.name.equals("agency") &&
            lineNumber > 2 &&
            transitIds
                .stream()
                .filter(transitId -> transitId.contains("agency_id"))
                .count() != lineNumber - 1
        ) {
            // The check on the agency table is carried out whilst the agency table is being loaded so it
            // is possible to compare the number of transitIds added against the number of rows loaded to
            // accurately determine missing agency_id values.
            String message = String.format(
                "%s is conditionally required when there is more than one agency.",
                check.conditionalFieldName
            );
            errors.add(
                NewGTFSError.forLine(table, lineNumber, CONDITIONALLY_REQUIRED, message)
            );
        } else if (
            (
                table.name.equals("routes") ||
                table.name.equals("fare_attributes")
            ) &&
                transitIds
                    .stream()
                    .filter(transitId -> transitId.contains("agency_id"))
                    .count() > 1 &&
                check.conditionalCheck == FIELD_NOT_EMPTY &&
                POSTGRES_NULL_TEXT.equals(conditionalFieldValue)
        ) {
            // By this point the agency table has already been loaded, therefore, if the number of agency_id
            // transitIds is greater than one it is assumed more than one agency has been provided.
            // FIXME: This doesn't work if only one agency_id is defined in the agency table. e.g. 2 rows of
            //  data, but the first doesn't define an agency_id.
            String message = String.format(
                "%s is conditionally required when there is more than one agency.",
                check.conditionalFieldName
            );
            errors.add(
                NewGTFSError.forLine(
                    table,
                    lineNumber,
                    AGENCY_ID_REQUIRED_FOR_TABLES_WITH_MORE_THAN_ONE_RECORD,
                    message).setEntityId(entityId)
            );
        }
        return errors;
    }

    /**
     * If the reference field value is within a defined range and the conditional field value has not be defined, flag
     * an error.
     */
    public static Set<NewGTFSError> checkFieldInRange(
        Table table,
        int lineNumber,
        Field referenceField,
        ConditionalRequirement check,
        String referenceFieldValue,
        String conditionalFieldValue,
        String entityId
    ) {
        Set<NewGTFSError> errors = new HashSet<>();

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
            check.conditionalCheck == FIELD_NOT_EMPTY &&
                POSTGRES_NULL_TEXT.equals(conditionalFieldValue);

        if (conditionallyRequiredValueIsEmpty) {
            // Reference value in range and conditionally required field is empty.
            String message = String.format(
                "%s is conditionally required when %s value is between %d and %d.",
                check.conditionalFieldName,
                referenceField.name,
                check.minReferenceValue,
                check.maxReferenceValue
            );
            errors.add(
                NewGTFSError.forLine(
                    table,
                    lineNumber,
                    CONDITIONALLY_REQUIRED,
                    message).setEntityId(entityId)
            );
        }
        return errors;
    }

    /**
     * Check that an expected foreign field value matches a conditional field value. Selected foreign field values are
     * added to {@link ReferenceTracker#foreignFieldIds} as part of the load process and are used here to check
     * conditional fields which have a dependency on them.
     */
    public static Set<NewGTFSError> checkForeignFieldValueMatch(
        Table table,
        int lineNumber,
        Field referenceField,
        ConditionalRequirement check,
        String referenceFieldValue,
        Set<String> foreignFieldIds,
        String entityId
    ) {
        Set<NewGTFSError> errors = new HashSet<>();
        // Expected reference in foreign field id list.
        String foreignFieldReference =
            String.join(
                ":",
                check.conditionalFieldName,
                referenceFieldValue
            );
        if (table.name.equals("fare_rules") &&
            !POSTGRES_NULL_TEXT.equals(referenceFieldValue) &&
            foreignFieldIds
                .stream()
                .noneMatch(id -> id.contains(foreignFieldReference))
        ) {
            // The foreign key reference required by fields in fare rules is not available in stops.
            String message = String.format(
                "%s %s is conditionally required in stops when referenced by %s in %s.",
                check.conditionalFieldName,
                referenceFieldValue,
                referenceField.name,
                table.name
            );
            errors.add(
                NewGTFSError.forLine(
                    table,
                    lineNumber,
                    CONDITIONALLY_REQUIRED,
                    message).setEntityId(entityId)
            );
        }
        return errors;
    }

    /**
     * Check the conditional field value, if it is empty the reference field value must be provided.
     */
    public static Set<NewGTFSError> checkFieldIsEmpty(
        Table table,
        int lineNumber,
        Field referenceField,
        ConditionalRequirement check,
        String referenceFieldValue,
        String conditionalFieldValue,
        String entityId
    ) {
        Set<NewGTFSError> errors = new HashSet<>();
        if (
            POSTGRES_NULL_TEXT.equals(conditionalFieldValue) &&
            POSTGRES_NULL_TEXT.equals(referenceFieldValue)
        ) {
            // The reference field is required when the conditional field is empty.
            String message = String.format(
                "%s is conditionally required when %s is empty.",
                referenceField.name,
                check.conditionalFieldName
            );
            errors.add(
                NewGTFSError.forLine(
                    table,
                    lineNumber,
                    CONDITIONALLY_REQUIRED,
                    message).setEntityId(entityId)
            );

        }
        return errors;
    }

    /**
     * Check the conditional field value is not empty and matches the expected value.
     */
    public static Set<NewGTFSError> checkFieldNotEmptyAndMatchesValue(
        Table table,
        int lineNumber,
        Field referenceField,
        ConditionalRequirement check,
        String referenceFieldValue,
        String conditionalFieldValue,
        String entityId
    ) {
        Set<NewGTFSError> errors = new HashSet<>();
        if (
            !POSTGRES_NULL_TEXT.equals(conditionalFieldValue) &&
            conditionalFieldValue.equals(check.conditionalFieldValue) &&
            POSTGRES_NULL_TEXT.equals(referenceFieldValue)
        ) {
            String message = String.format(
                "%s is conditionally required when %s is provided and matches %s.",
                referenceField.name,
                check.conditionalFieldName,
                check.conditionalFieldValue
            );
            errors.add(
                NewGTFSError.forLine(
                    table,
                    lineNumber,
                    CONDITIONALLY_REQUIRED,
                    message).setEntityId(entityId)
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
