package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.TestUtils;
import com.conveyal.gtfs.loader.FeedLoadResult;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;

import static com.conveyal.gtfs.GTFS.*;
import static com.conveyal.gtfs.TestUtils.assertThatSqlCountQueryYieldsExpectedCount;

public class TravelTooFastTest {
    private static String testDBName;
    private static DataSource testDataSource;
    private static String testNamespace;

    @BeforeClass
    public static void setUpClass() {
        // create a new database
        testDBName = TestUtils.generateNewDB();
        String dbConnectionUrl = String.format("jdbc:postgresql://localhost/%s", testDBName);
        testDataSource = TestUtils.createTestDataSource(dbConnectionUrl);
    }

    @AfterClass
    public static void tearDownClass() {
        TestUtils.dropDB(testDBName);
    }

    /**
     * Can validate the travel speed between stops which have different frequency of arrival and departure times.
     * The stop_times are in three parts:
     * 1. A sequence of stops with arrival and departure times. To confirm that the travel speed can be calculated when
     * all the stops provide arrival and departure times.
     * 2. A sequence of stops where only the first and last stop have an arrival and departure time. To confirm that the
     * travel speed can be calculated when intermediate stops have no arrival/departure times.
     * 3. A sequence of stops where the first and last stop have arrival and departure times only a minute apart. To
     * confirm that the travel speed can be calculated when intermediate stops have no arrival/departure times and that
     * the 'TRAVEL_TOO_FAST' error is logged.
     */
    @Test
    public void canValidateTravelSpeedBetweenStops() throws Exception {
        String zipFileName = TestUtils.zipFolderFiles("real-world-gtfs-feeds/VTA-gtfs-single-trip", true);
        // load feed into db
        FeedLoadResult feedLoadResult = load(zipFileName, testDataSource);
        testNamespace = feedLoadResult.uniqueIdentifier;
        // validate feed to create additional tables
        validate(testNamespace, testDataSource);

        assertThatSqlCountQueryYieldsExpectedCount(
            testDataSource,
            String.format("select count(*) from %s.errors where error_type = 'TRAVEL_TOO_FAST'", testNamespace),
            1);
    }
}
