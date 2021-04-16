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
        checkFeedHasError(CONDITIONALLY_REQUIRED, "stops.txt, stop_name is required for id 4957.");
    }

    @Test
    public void stopTableMissingConditionallyRequiredParentStation() {
        checkFeedHasError(CONDITIONALLY_REQUIRED, "stops.txt, parent_station is required for id 691.");
    }

    @Test
    public void stopTableMissingConditionallyRequiredStopLat() {
        checkFeedHasError(CONDITIONALLY_REQUIRED, "stops.txt, stop_lat is required for id 691.");
    }

    @Test
    public void stopTableMissingConditionallyRequiredStopLon() {
        checkFeedHasError(CONDITIONALLY_REQUIRED, "stops.txt, stop_long is required for id 692.");
    }

    @Test
    public void stopTableMissingConditionallyRequiredZoneId() {
        checkFeedHasError(CONDITIONALLY_REQUIRED, "stops.txt, zone_id 1 is required by fare_rules.txt.");
    }

    /**
     * Check that the test feed has exactly one error for the given type and badValue.
     */
    private void checkFeedHasError(NewGTFSErrorType type, String badValue) {
        assertThatSqlCountQueryYieldsExpectedCount(
            testDataSource,
            String.format("select count(*) from %s.errors where error_type = '%s' and bad_value = '%s'",
                testNamespace,
                type,
                badValue),
            1);
    }

}
