package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.error.NewGTFSError;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.conveyal.gtfs.error.NewGTFSErrorType.CONDITIONALLY_REQUIRED;
import static com.conveyal.gtfs.error.NewGTFSErrorType.DUPLICATE_ID;
import static com.conveyal.gtfs.error.NewGTFSErrorType.REFERENTIAL_INTEGRITY;
import static com.conveyal.gtfs.loader.ConditionalCheckType.FIELD_IN_RANGE;
import static com.conveyal.gtfs.loader.ConditionalCheckType.FIELD_NOT_EMPTY;
import static com.conveyal.gtfs.loader.JdbcGtfsLoader.POSTGRES_NULL_TEXT;

/**
 * This class is used while loading GTFS to track the unique keys that are encountered in a GTFS
 * feed. It has two sets of strings that it tracks, one for single field keys (e.g., route_id or
 * stop_id) and one for keys that are compound, usually made up of a string ID with a sequence field
 * (e.g., trip_id + stop_sequence for tracking unique stop times).
 * <p>
 * NOTE: Its methods should remain public because they are used during external processes that
 * validate or otherwise iterate over each line of a GTFS file and need to check for reference
 * validity (e.g., while merging GTFS feeds this is used to determine ID conflicts).
 */
public class ReferenceTracker {
    public final Set<String> transitIds = new HashSet<>();
    public final Set<String> transitIdsWithSequence = new HashSet<>();

    /**
     * During table load, checks the uniqueness of the entity ID and that references are valid.
     * NOTE: This method defaults the key field and order field names to this table's values.
     *
     * @param keyValue   key value for the record being checked
     * @param lineNumber line number of the record being checked
     * @param field      field currently being checked
     * @param value      value that corresponds to field
     * @param table      table currently being checked
     * @return any duplicate or bad reference errors.
     */
    public Set<NewGTFSError> checkReferencesAndUniqueness(String keyValue, int lineNumber,
        Field field, String value, Table table) {
        return checkReferencesAndUniqueness(keyValue, lineNumber, field, value, table,
            table.getKeyFieldName(), table.getOrderFieldName());
    }

    /**
     * During table load, checks the uniqueness of the entity ID and that references are valid.
     * These references are stored in the provided reference tracker. Any non-unique IDs or invalid
     * references will store an error. NOTE: this instance of checkReferencesAndUniqueness allows
     * for arbitrarily setting the keyField and orderField, which is helpful for checking uniqueness
     * of fields that are not the standard primary key (e.g., route_short_name).
     */
    public Set<NewGTFSError> checkReferencesAndUniqueness(String keyValue, int lineNumber,
        Field field, String value, Table table, String keyField, String orderField) {
        Set<NewGTFSError> errors = new HashSet<>();
        // Store field-scoped transit ID for referential integrity check. (Note, entity scoping
        // doesn't work here because we need to cross-check multiple entity types for valid
        // references, e.g., stop times and trips both share trip id.)
        // If table has an order field, that order field should supersede the key field as the
        // "unique" field. In other words, if it has an order field, the unique key is actually
        // compound -- made up of the keyField + orderField.
        String uniqueKeyField = orderField != null ? orderField
            // If table has no unique key field (e.g., calendar_dates or transfers), there is no
            // need to check for duplicates.
            : !table.hasUniqueKeyField ? null : keyField;
        String transitId = String.join(":", keyField, keyValue);

        // If the field is optional and there is no value present, skip check.
        if (!field.isRequired() && "".equals(value)) return Collections.emptySet();

        // First, handle referential integrity check.
        boolean isOrderField = field.name.equals(orderField);
        if (field.isForeignReference()) {
            // Check referential integrity if the field is a foreign reference. Note: the
            // reference table must be loaded before the table/value being currently checked.
            String referenceField = field.referenceTable.getKeyFieldName();
            String referenceTransitId = String.join(":", referenceField, value);

            if (!this.transitIds.contains(referenceTransitId)) {
                // If the reference tracker does not contain
                NewGTFSError referentialIntegrityError = NewGTFSError
                    .forLine(table, lineNumber, REFERENTIAL_INTEGRITY, referenceTransitId)
                    .setEntityId(keyValue);
                // If the field is an order field, set the sequence for the new error.
                if (isOrderField) referentialIntegrityError.setSequence(value);
                errors.add(referentialIntegrityError);
            }
        }
        // Next, handle duplicate ID check.
        // In most cases there is no need to check for duplicate IDs if the field is a foreign
        // reference. However, transfers#to_stop_id is defined as an order field, so we need to
        // check that this field (which is both a foreign ref and order field) is dataset unique
        // in conjunction with the key field.
        // These hold references to the set of IDs to check for duplicates and the ID to check.
        // These depend on whether an order field is part of the "unique ID."
        Set<String> listOfUniqueIds = this.transitIds;
        String uniqueId = transitId;

        // Next, check that the ID is table-unique. For example, the trip_id field is table unique
        // in trips.txt and the the stop_sequence field (joined with trip_id) is table unique in
        // stop_times.txt.
        if (field.name.equals(uniqueKeyField)) {
            // Check for duplicate IDs and store entity-scoped IDs for referential integrity check
            if (isOrderField) {
                // Check duplicate reference in set of field-scoped id:sequence (e.g.,
                // stop_sequence:12345:2)
                // This should not be scoped by key field because there may be conflicts (e.g.,
                // with trip_id="12345:2")
                listOfUniqueIds = this.transitIdsWithSequence;
                uniqueId = String.join(":", field.name, keyValue, value);
            }
            if (table.required.equals(Requirement.PROPRIETARY)) {
                // Some proprietary tables in the GTFS+ spec do not conform to the general principle in GTFS where a key
                // field (e.g., stop_id) only acts as the primary key field in the entity's table. For example, stop_id
                // acts as a primary key on stop_attributes.txt, so we prepend the table name to the unique ID for these
                // tables when checking for duplicate entries.
                uniqueId = String.join(":", table.name, uniqueId);
            }
             // Add ID and check duplicate reference in entity-scoped IDs (e.g., stop_id:12345)
            boolean valueAlreadyExists = !listOfUniqueIds.add(uniqueId);
            if (valueAlreadyExists) {
                // If the value is a duplicate, add an error.
                NewGTFSError duplicateIdError =
                    NewGTFSError.forLine(table, lineNumber, DUPLICATE_ID, uniqueId)
                        .setEntityId(keyValue);
                if (isOrderField) { duplicateIdError.setSequence(value); }
                errors.add(duplicateIdError);
            }
        } else if (
            field.name.equals(keyField) &&
            (!field.isForeignReference() || Table.CALENDAR_DATES.name.equals(table.name))
        ) {
            // We arrive here if the field is not a foreign reference and not the unique key field
            // on the table (e.g., shape_pt_sequence), but is still a key on the table. For
            // example, this is where we add shape_id from the shapes table, so that when we
            // check the referential integrity of trips#shape_id, we know that the shape_id
            // exists in the shapes table. It also handles tracking calendar_dates#service_id values.
            listOfUniqueIds.add(uniqueId);
        }
        return errors;
    }


    /**
     * Work through each conditionally required check assigned to a table. First check the reference field to confirm
     * if it meets the conditions whereby the conditional field is required. If the conditional field is required confirm
     * that a value has been provided, if not, log an error.
     */
    public Set<NewGTFSError> checkConditionallyRequiredFields(Table table, Field[] fields, String[] rowData, int lineNumber) {
        Set<NewGTFSError> errors = new HashSet<>();
        Map<Field, ConditionalRequirement[]> fieldsToCheck = table.getConditionalRequirements();
        for (Map.Entry<Field, ConditionalRequirement[]> entry : fieldsToCheck.entrySet()) {
            Field referenceField = entry.getKey();
            ConditionalRequirement[] conditionalRequirements = entry.getValue();
            for (ConditionalRequirement check : conditionalRequirements) {
                int refFieldIndex = Field.getFieldIndex(fields, referenceField.name);
                String refFieldData = getValueForRow(rowData, refFieldIndex);
                boolean referenceValueMeetsRangeCondition = check.referenceCheck == FIELD_IN_RANGE &&
                    !POSTGRES_NULL_TEXT.equals(refFieldData) &&
                    // TODO use pre-existing method in ShortField?
                    isValueInRange(refFieldData, check.minReferenceValue, check.maxReferenceValue);
                // If ref value does not meet the range condition, there is no need to check this conditional value for
                // (e.g.) an empty value. Continue to the next check.
                if (!referenceValueMeetsRangeCondition) continue;
                int conditionalFieldIndex = Field.getFieldIndex(fields, check.conditionalFieldName);
                String conditionalFieldData = getValueForRow(rowData, conditionalFieldIndex);
                boolean conditionallyRequiredValueIsEmpty = check.conditionalCheck == FIELD_NOT_EMPTY &&
                    POSTGRES_NULL_TEXT.equals(conditionalFieldData);
                if (conditionallyRequiredValueIsEmpty) {
                    String entityId = getValueForRow(rowData, table.getKeyFieldIndex(fields));
                    String message = String.format(
                        "%s is conditionally required when %s value is between %d and %d.",
                        check.conditionalFieldName,
                        referenceField.name,
                        check.minReferenceValue,
                        check.maxReferenceValue
                    );
                    errors.add(
                        NewGTFSError.forLine(table, lineNumber, CONDITIONALLY_REQUIRED, message).setEntityId(entityId)
                    );
                }
            }
        }
        return errors;
    }

    /**
     * Get value for a particular column index from a set of row data. Note: the row data here has one extra value at
     * the beginning of the array that represents the line number (hence the +1). This is because the data is formatted
     * for batch insertion into a postgres table.
     */
    private String getValueForRow(String[] rowData, int columnIndex) {
        return rowData[columnIndex + 1];
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

    /**
     * Checks if the provided field value is empty. If the value can be converted to either a double or int and these
     * match the minimum value it is assumed these are empty.
     */
    private boolean isEmpty(String str) {
        // Text values
        if (str == null || str.isEmpty()) {
            return true;
        }

        // Number values
        if (NumberUtils.isParsable(str)) {
            try {
                double dValue = Double.parseDouble(str);
                if (dValue == Double.MIN_VALUE) {
                    return true;
                }
                int iValue = Integer.parseInt(str);
                if (iValue == Integer.MIN_VALUE) {
                    return true;
                }
                int sValue = Short.parseShort(str);
                if (sValue == Short.MIN_VALUE) {
                    return true;
                }
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return false;
    }
}
