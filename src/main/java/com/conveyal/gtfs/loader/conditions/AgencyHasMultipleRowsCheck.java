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
 * Conditional requirement to check that an agency_id has been provided if more than one row exists in agency.txt.
 */
public class AgencyHasMultipleRowsCheck extends ConditionalRequirement {

    private final int FIRST_ROW = 2;
    private final int SECOND_ROW = 3;

    public AgencyHasMultipleRowsCheck() {
        this.dependentFieldName = "agency_id";
        this.dependentFieldCheck = HAS_MULTIPLE_ROWS;
    }

    /**
     * Flag an error if there are multiple rows in agency.txt and the agency_id is missing for any rows.
     */
    public Set<NewGTFSError> check(
        LineContext lineContext,
        Field referenceField,
        HashMultimap<String, String> uniqueValuesForFields
    ) {
        String dependentFieldValue = lineContext.getValueForRow(dependentFieldName);
        Set<NewGTFSError> errors = new HashSet<>();
        Set<String> agencyIdValues = uniqueValuesForFields.get(dependentFieldName);
        // Do some awkward checks to determine if the first or second row (or another) is missing the agency_id.
        boolean firstOrSecondMissingId = lineContext.lineNumber == SECOND_ROW && agencyIdValues.contains("");
        boolean currentRowMissingId = POSTGRES_NULL_TEXT.equals(dependentFieldValue);
        boolean secondRowMissingId = firstOrSecondMissingId && currentRowMissingId;
        if (firstOrSecondMissingId || (lineContext.lineNumber > SECOND_ROW && currentRowMissingId)) {
            // The check on the agency table is carried out whilst the agency table is being loaded so it
            // is possible to compare the number of agencyIdValues added against the number of rows loaded to
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
                    dependentFieldName
                )
            );
        }
        return errors;
    }

}
