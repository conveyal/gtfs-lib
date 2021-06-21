package com.conveyal.gtfs.loader.conditions;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.loader.Field;
import com.conveyal.gtfs.loader.LineContext;
import com.conveyal.gtfs.loader.ReferenceTracker;
import com.google.common.collect.HashMultimap;

import java.util.HashSet;
import java.util.Set;

import static com.conveyal.gtfs.error.NewGTFSErrorType.REFERENTIAL_INTEGRITY;
import static com.conveyal.gtfs.loader.JdbcGtfsLoader.POSTGRES_NULL_TEXT;

/**
 * Conditional requirement to check that an expected foreign field value matches a conditional field value.
 */
public class ForeignRefExistsCheck extends ConditionalRequirement {
    /** The reference table name. */
    private String referenceTableName;

    public ForeignRefExistsCheck(String dependentFieldName, String referenceTableName) {
        this.dependentFieldName = dependentFieldName;
        this.referenceTableName = referenceTableName;
    }

    /**
     * Check that an expected foreign field value matches a conditional field value. Selected foreign field values are
     * added to {@link ReferenceTracker#uniqueValuesForFields} as part of the load process and are used here to check
     * conditional fields which have a dependency on them. e.g. stop#zone_id does not exist in stops table, but is
     * required by fare_rules records (e.g. origin_id).
     */
    public Set<NewGTFSError> check(
        LineContext lineContext,
        Field referenceField,
        HashMultimap<String, String> uniqueValuesForFields
    ) {
        Set<NewGTFSError> errors = new HashSet<>();
        String referenceFieldValue = lineContext.getValueForRow(referenceField.name);
        // Expected reference in foreign field id list.
        String foreignFieldReference =
            String.join(
                ":",
                dependentFieldName,
                referenceFieldValue
            );
        if (lineContext.table.name.equals(referenceTableName) &&
            !POSTGRES_NULL_TEXT.equals(referenceFieldValue) &&
            !uniqueValuesForFields.get(dependentFieldName).contains(foreignFieldReference)
        ) {
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

}
