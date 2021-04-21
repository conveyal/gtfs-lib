package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.TestUtils;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;

import java.io.IOException;

import static com.conveyal.gtfs.GTFS.load;
import static com.conveyal.gtfs.TestUtils.assertThatSqlCountQueryYieldsExpectedCount;
import static com.conveyal.gtfs.error.NewGTFSErrorType.CONDITIONALLY_REQUIRED;
import static com.conveyal.gtfs.error.NewGTFSErrorType.REFERENTIAL_INTEGRITY;

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
    }

    @AfterAll
    public static void tearDownClass() {
        TestUtils.dropDB(testDBName);
    }

    @Test
    public void stopTableMissingConditionallyRequiredStopName() {
        checkFeedHasOneError(CONDITIONALLY_REQUIRED, "Stop","2", "4957","stop_name is conditionally required.");
    }

    @Test
    public void stopTableMissingConditionallyRequiredParentStation() {
        checkFeedHasOneError(CONDITIONALLY_REQUIRED, "Stop","5", "1266","parent_station is conditionally required.");
    }

    @Test
    public void stopTableMissingConditionallyRequiredStopLat() {
        checkFeedHasOneError(CONDITIONALLY_REQUIRED, "Stop","3", "691","stop_lat is conditionally required.");
    }

    @Test
    public void stopTableMissingConditionallyRequiredStopLon() {
        checkFeedHasOneError(CONDITIONALLY_REQUIRED, "Stop","4", "692","stop_lon is conditionally required.");
    }

    @Test
    public void stopTableMissingConditionallyRequiredZoneId() {
        checkFeedHasOneError(REFERENTIAL_INTEGRITY, "stop_id:1");
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
     * Check that the test feed has exactly one error for the given error type and badValue.
     */
    private void checkFeedHasOneError(NewGTFSErrorType errorType, String badValue) {
        assertThatSqlCountQueryYieldsExpectedCount(
            testDataSource,
            String.format("select count(*) from %s.errors where error_type = '%s' and bad_value = '%s'",
                testNamespace,
                errorType,
                badValue),
            1);
    }
}
