package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit tests to verify functionality of classes that load fields from GTFS tables.
 */
public class FieldTests {

    /**
     * Make sure our date field reader catches bad dates and accepts correct ones.
     */
    @Test
    public void dateFieldParseTest() {

        String[] badDates = {
                "20170733", // 33rd day of the month
                "20171402", // 14th months of the year
                "12252016", // month day year format
                "14921212",  // Year very far in the past
                "2015/03/11", // Slashes separating fields
                "2011-07-16", // Dashes separating fields
                "23000527", // Very distant future year
                "790722" // Two digit year
        };
        DateField dateField = new DateField("date", Requirement.REQUIRED);
        for (String badDate : badDates) {
            ValidateFieldResult<String> result = dateField.validateAndConvert(badDate);
            assertThat("Parsing bad dates should result in an error.", result.errors.size() > 0);
            NewGTFSError error = result.errors.iterator().next();
            assertThat("Error type should be date-related.",
                       error.errorType == NewGTFSErrorType.DATE_FORMAT || error.errorType == NewGTFSErrorType.DATE_RANGE);
            assertThat("Error's bad value should be the input value (the bad date).", error.badValue.equals(badDate));
        }

        String[] goodDates = { "20160522", "20180717", "20221212", "20190505" };

        for (String goodDate : goodDates) {
            ValidateFieldResult<String> result = dateField.validateAndConvert(goodDate);
            assertThat("Returned value matches the well-formed date.", result.clean.equals(goodDate));
        }

    }

    /**
     * Make sure {@link Field#cleanString(ValidateFieldResult)} catches and removes illegal character sequences.
     */
    @Test
    public void illegalCharacterParseTest() {
        String[] badStrings = {
            "\n", // simple new line
            "\t", // simple tab
            "\t\n\r", // new line, tab, carriage return
            "Hello\\world",  // backslashes not permitted (replaced with escaped slash)
            "Downtown via Peachtree\n\nSt" // new line and carriage within string
        };
        StringField stringField = new StringField("any", Requirement.REQUIRED);
        for (String badString : badStrings) {
            ValidateFieldResult<String> result = stringField.validateAndConvert(badString);
            assertThat("Input with illegal characters should result in an error.", result.errors.size() > 0);
            NewGTFSError error = result.errors.iterator().next();
            assertThat("Error type should be illegal field value.",
                       error.errorType == NewGTFSErrorType.ILLEGAL_FIELD_VALUE);
            for (IllegalCharacter illegalCharacter : Field.ILLEGAL_CHARACTERS) {
                // Check that string is clean. Note: for backslash, we check that the clean string contains an escaped
                // backslash because checking for a single backslash will yield true even for after a successful
                // substitution.
                boolean stringIsClean = !result.clean.contains(illegalCharacter.illegalSequence);
                if (illegalCharacter.illegalSequence.equals("\\") && !stringIsClean) {
                    stringIsClean = result.clean.contains(illegalCharacter.replacement);
                }
                assertThat(String.format("The cleaned string '%s' should not contain illegal character (%s).", result.clean, illegalCharacter.illegalSequence), stringIsClean);
            }
        }
    }
}
