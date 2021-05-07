package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.TestUtils;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.stream.Stream;

import static com.conveyal.gtfs.GTFS.load;
import static com.conveyal.gtfs.GTFS.validate;
import static com.conveyal.gtfs.TestUtils.assertThatSqlCountQueryYieldsExpectedCount;
import static com.conveyal.gtfs.error.NewGTFSErrorType.AGENCY_ID_REQUIRED_FOR_MULTI_AGENCY_FEEDS;
import static com.conveyal.gtfs.error.NewGTFSErrorType.CONDITIONALLY_REQUIRED;

public class ConditionallyRequiredTest {
    private static String testDBName;
    private static DataSource testDataSource;
    private static String testNamespace;

    @BeforeAll
    public static void setUpClass() throws IOException {
        // Create a new database
        testDBName = TestUtils.generateNewDB();
        String dbConnectionUrl = String.format("jdbc:postgresql://localhost/%s", testDBName);
        testDataSource = TestUtils.createTestDataSource(dbConnectionUrl);
        // load feed into db
        String zipFileName = TestUtils.zipFolderFiles("real-world-gtfs-feeds/VTA-gtfs-conditionally-required-checks", true);
        FeedLoadResult feedLoadResult = load(zipFileName, testDataSource);
        testNamespace = feedLoadResult.uniqueIdentifier;
        validate(testNamespace, testDataSource);
    }

    @AfterAll
    public static void tearDownClass() {
        TestUtils.dropDB(testDBName);
    }

    @Test
    public void stopTableMissingConditionallyRequiredStopName() {
        checkFeedHasOneError(CONDITIONALLY_REQUIRED, "Stop","2", "4957","stop_name is conditionally required when location_type value is between 0 and 2.");
    }

    @Test
    public void stopTableMissingConditionallyRequiredParentStation() {
        checkFeedHasOneError(CONDITIONALLY_REQUIRED, "Stop","5", "1266","parent_station is conditionally required when location_type value is between 2 and 4.");
    }

    @Test
    public void stopTableMissingConditionallyRequiredStopLat() {
        checkFeedHasOneError(CONDITIONALLY_REQUIRED, "Stop","3", "691","stop_lat is conditionally required when location_type value is between 0 and 2.");
    }

    @Test
    public void stopTableMissingConditionallyRequiredStopLon() {
        checkFeedHasOneError(CONDITIONALLY_REQUIRED, "Stop","4", "692","stop_lon is conditionally required when location_type value is between 0 and 2.");
    }

    @ParameterizedTest
    @MethodSource("createZoneIdDependencies")
    public void stopTableMissingConditionallyRequiredZoneId(String entityType, String lineNumber, String entityId, String badValue) {
        checkFeedHasOneError(CONDITIONALLY_REQUIRED, entityType, lineNumber, entityId, badValue);
    }

    private static Stream<Arguments> createZoneIdDependencies() {
        return Stream.of(
            Arguments.of("FareRule", "3", "1", "zone_id 4 is conditionally required in stops when referenced by contains_id in fare_rules."),
            Arguments.of("FareRule", "3", "1", "zone_id 3 is conditionally required in stops when referenced by destination_id in fare_rules."),
            Arguments.of("FareRule", "3", "1", "zone_id 2 is conditionally required in stops when referenced by origin_id in fare_rules.")
        );
    }


    @Test
    public void agencyTableMissingConditionallyRequiredAgencyId() {
        checkFeedHasOneError(CONDITIONALLY_REQUIRED, "Agency","3", "agency_id is conditionally required when there is more than one agency.");
    }

    @Test
    public void tripTableMissingConditionallyRequiredShapeId() {
        checkFeedHasOneError(CONDITIONALLY_REQUIRED, "Trip","2", "1","shape_id is conditionally required when a trip has continuous behavior defined.");
    }

    @Test
    public void stopTimeTableMissingConditionallyRequiredArrivalDepartureTimes() {
        checkFeedHasOneError(CONDITIONALLY_REQUIRED, "StopTime","10", "1","First and last stop times are conditionally required to have both an arrival and departure time.");
    }

    @Test
    public void routeTableMissingConditionallyRequiredAgencyId() {
        checkFeedHasOneError(AGENCY_ID_REQUIRED_FOR_MULTI_AGENCY_FEEDS, "Route","2", "21","agency_id is conditionally required when there is more than one agency.");
    }

    @Test
    public void fareAttributeTableMissingConditionallyRequiredAgencyId() {
        checkFeedHasOneError(AGENCY_ID_REQUIRED_FOR_MULTI_AGENCY_FEEDS, "FareAttribute","2", "1","agency_id is conditionally required when there is more than one agency.");
    }

    /**
     * Check that the test feed has exactly one error for the provided values.
     */
    private void checkFeedHasOneError(NewGTFSErrorType errorType, String entityType, String lineNumber, String entityId, String badValue) {
        assertThatSqlCountQueryYieldsExpectedCount(
            testDataSource,
            String.format("select count(*) from %s.errors where error_type = '%s' and entity_type = '%s' and line_number = '%s' and entity_id = '%s' and bad_value = '%s'",
                testNamespace,
                errorType,
                entityType,
                lineNumber,
                entityId,
                badValue),
            1);
    }

    /**
     * Check that the test feed has exactly one error for the provided values.
     */
    private void checkFeedHasOneError(NewGTFSErrorType errorType, String entityType, String lineNumber, String badValue) {
        assertThatSqlCountQueryYieldsExpectedCount(
            testDataSource,
            String.format("select count(*) from %s.errors where error_type = '%s' and entity_type = '%s' and line_number = '%s' and bad_value = '%s'",
                testNamespace,
                errorType,
                entityType,
                lineNumber,
                badValue),
            1);
    }
}
