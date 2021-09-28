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
import static com.conveyal.gtfs.TestUtils.checkFeedHasExpectedNumberOfErrors;
import static com.conveyal.gtfs.error.NewGTFSErrorType.CONDITIONALLY_REQUIRED;
import static com.conveyal.gtfs.error.NewGTFSErrorType.REFERENTIAL_INTEGRITY;
import static com.conveyal.gtfs.error.NewGTFSErrorType.AGENCY_ID_REQUIRED_FOR_MULTI_AGENCY_FEEDS;

public class ConditionallyRequiredTest {
    private static String VTADBName;
    private static DataSource VTADataSource;
    private static String VTANamespace;

    private static String triDeltaDBName;
    private static DataSource triDeltaDataSource;
    private static String triDeltaNamespace;

    @BeforeAll
    public static void setUpClass() throws IOException {
        VTADBName = TestUtils.generateNewDB();
        VTADataSource = TestUtils.createTestDataSource(String.format("jdbc:postgresql://localhost/%s", VTADBName));
        VTANamespace = loadFeedAndValidate(VTADataSource, "real-world-gtfs-feeds/VTA-gtfs-conditionally-required-checks");

        triDeltaDBName = TestUtils.generateNewDB();
        triDeltaDataSource = TestUtils.createTestDataSource(String.format("jdbc:postgresql://localhost/%s", triDeltaDBName));
        triDeltaNamespace = loadFeedAndValidate(triDeltaDataSource, "real-world-gtfs-feeds/tri-delta-fare-rules");
    }

    /**
     * Load feed from zip file into a database and validate.
     */
    private static String loadFeedAndValidate(DataSource dataSource, String zipFolderName) throws IOException {
        String zipFileName = TestUtils.zipFolderFiles(zipFolderName,  true);
        FeedLoadResult feedLoadResult = load(zipFileName, dataSource);
        String namespace = feedLoadResult.uniqueIdentifier;
        validate(namespace, dataSource);
        // return name space.
        return namespace;
    }

    @AfterAll
    public static void tearDownClass() {
        TestUtils.dropDB(VTADBName);
        TestUtils.dropDB(triDeltaDBName);
    }

    @Test
    public void stopTimeTableMissingConditionallyRequiredArrivalDepartureTimes() {
        checkFeedHasOneError(
            CONDITIONALLY_REQUIRED,
            "StopTime",
            "10",
            "1",
            "First and last stop times are required to have both an arrival and departure time."
        );
    }

    @ParameterizedTest
    @MethodSource("createStopTableChecks")
    public void stopTableConditionallyRequiredTests(
        NewGTFSErrorType errorType,
        String entityType,
        String lineNumber,
        String entityId,
        String badValue
    ) {
        checkFeedHasOneError(errorType, entityType, lineNumber, entityId, badValue);
    }

    private static Stream<Arguments> createStopTableChecks() {
        return Stream.of(
            Arguments.of(CONDITIONALLY_REQUIRED, "Stop", "2", "4957", "stop_name is required when location_type value is between 0 and 2."),
            Arguments.of(CONDITIONALLY_REQUIRED, "Stop", "5", "1266", "parent_station is required when location_type value is between 2 and 4."),
            Arguments.of(CONDITIONALLY_REQUIRED, "Stop", "3", "691", "stop_lat is required when location_type value is between 0 and 2."),
            Arguments.of(CONDITIONALLY_REQUIRED, "Stop", "4", "692", "stop_lon is required when location_type value is between 0 and 2."),
            Arguments.of(REFERENTIAL_INTEGRITY, "FareRule", "3", "1", "contains_id:zone_id:4"),
            Arguments.of(REFERENTIAL_INTEGRITY, "FareRule", "3", "1", "destination_id:zone_id:3"),
            Arguments.of(REFERENTIAL_INTEGRITY, "FareRule", "3", "1", "origin_id:zone_id:2")
        );
    }

    @ParameterizedTest
    @MethodSource("createTranslationTableChecks")
    public void translationTableConditionallyRequiredTests(
        String entityType,
        String lineNumber,
        String entityId,
        String badValue
    ) {
        checkFeedHasOneError(CONDITIONALLY_REQUIRED, entityType, lineNumber, entityId, badValue);
    }

    private static Stream<Arguments> createTranslationTableChecks() {
        return Stream.of(
            Arguments.of("Translation", "2", "stops", "record_id is required when field_value is empty."),
            Arguments.of("Translation", "3", "stops", "field_value is required when record_id is empty."),
            Arguments.of("Translation", "4", "stops", "record_sub_id is required and must match stop_times when record_id is provided.")
        );
    }

    @Test
    public void agencyTableMissingConditionallyRequiredAgencyId() {
        checkFeedHasOneError(
            AGENCY_ID_REQUIRED_FOR_MULTI_AGENCY_FEEDS,
            "Agency",
            "2",
            null,
            "agency_id");
    }

    @Test
    public void tripTableMissingConditionallyRequiredShapeId() {
        checkFeedHasOneError(
            CONDITIONALLY_REQUIRED,
            "Trip",
            "2",
            "1",
            "shape_id is required when a trip has continuous behavior defined."
        );
    }

    @Test
    public void routeTableMissingConditionallyRequiredAgencyId() {
        checkFeedHasOneError(
            AGENCY_ID_REQUIRED_FOR_MULTI_AGENCY_FEEDS,
            "Route",
            "2",
            "21",
            null
        );
    }

    @Test
    public void fareAttributeTableMissingConditionallyRequiredAgencyId() {
        checkFeedHasOneError(
            AGENCY_ID_REQUIRED_FOR_MULTI_AGENCY_FEEDS,
            "FareAttribute",
            "2",
            "1",
            null
        );
    }

    @Test
    void shouldNotTriggerRefIntegrityError() {
        checkFeedHasExpectedNumberOfErrors(
            triDeltaNamespace,
            triDeltaDataSource,
            REFERENTIAL_INTEGRITY,
            "FareRule",
            "2",
            "1",
            null,
            0
        );
    }

    /**
     * Check that a test feed has exactly one error for the provided values.
     */
    private void checkFeedHasOneError(NewGTFSErrorType errorType, String entityType, String lineNumber, String entityId, String badValue) {
        checkFeedHasExpectedNumberOfErrors(
            VTANamespace,
            VTADataSource,
            errorType,
            entityType,
            lineNumber,
            entityId,
            badValue,
            1
        );
    }
}
