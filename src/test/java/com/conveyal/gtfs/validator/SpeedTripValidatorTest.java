package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.TestUtils;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.loader.FeedLoadResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;

import static com.conveyal.gtfs.GTFS.load;
import static com.conveyal.gtfs.GTFS.validate;
import static com.conveyal.gtfs.TestUtils.assertThatSqlCountQueryYieldsExpectedCount;
import static com.conveyal.gtfs.error.NewGTFSErrorType.TRAVEL_TOO_FAST;
import static com.conveyal.gtfs.error.NewGTFSErrorType.TRAVEL_TOO_SLOW;

/**
 * Distances recorded against each unit test have been produced using the lat/lon values from
 * real-world-gtfs-feeds/VTA-gtfs-multiple-trips/stops.txt matching the start and end of each trip. The distances were
 * calculated using https://www.geodatasource.com/distance-calculator.
 */
public class SpeedTripValidatorTest {
    private static String testDBName;
    private static DataSource testDataSource;
    private static String testNamespace;

    @BeforeAll
    public static void setUpClass() throws Exception {
        // create a new database
        testDBName = TestUtils.generateNewDB();
        String dbConnectionUrl = String.format("jdbc:postgresql://localhost/%s", testDBName);
        testDataSource = TestUtils.createTestDataSource(dbConnectionUrl);
        String zipFileName = TestUtils.zipFolderFiles("real-world-gtfs-feeds/VTA-gtfs-multiple-trips", true);
        // load feed into db
        FeedLoadResult feedLoadResult = load(zipFileName, testDataSource);
        testNamespace = feedLoadResult.uniqueIdentifier;
        // validate feed to create additional tables
        validate(testNamespace, testDataSource);
    }

    @AfterAll
    public static void tearDownClass() {
        TestUtils.dropDB(testDBName);
    }

    /**
     * Trip 1
     * Distance: 2.51km
     * Time: 8 minutes
     * Speed: 18.825 kph
     */
    @Test
    public void tripTravelingAtNormalSpeedWithAllStopTimesIsErrorFree() {
        checkFeedIsErrorFree(TRAVEL_TOO_FAST, "1");
        checkFeedIsErrorFree(TRAVEL_TOO_SLOW, "1");
    }

    /**
     * Trip 2
     * Distance: 1.21km
     * Time: 3 minutes
     * Speed: 24.4 kph
     */
    @Test
    public void tripTravelingAtNormalSpeedWithMissingStopTimesIsErrorFree() {
        checkFeedIsErrorFree(TRAVEL_TOO_FAST, "2");
        checkFeedIsErrorFree(TRAVEL_TOO_SLOW, "2");
    }

    /**
     * Trip 3
     * Distance: 3km
     * Time: 1 minute
     * Speed: 180 kph
     */
    @Test
    public void tripTravelingTooFastWithAllStopTimesHasError() {
        checkFeedHasError(TRAVEL_TOO_FAST, "3", 2);
    }

    /**
     * Trip 4
     * Distance: 3km
     * Time: 1 minute
     * Speed: 180 kph
     */
    @Test
    public void tripTravelingTooFastWithMissingStopTimesHasError() {
        checkFeedHasError(TRAVEL_TOO_FAST, "4", 4);
    }

    /**
     * Trip 5
     * Distance: 6.69km
     * Time: 23 hours, 59 minutes
     * Speed: 0.27 kph
     */
    @Test
    public void tripTravelingTooSlowWithAllStopTimesHasError() {
        checkFeedHasError(TRAVEL_TOO_SLOW, "5", 3);
    }

    /**
     * Trip 6
     * Distance: 6.69km
     * Time: 23 hours, 59 minutes
     * Speed: 0.27 kph
     */
    @Test
    public void tripTravelingTooSlowWithMissingStopTimesHasError() {
        checkFeedHasError(TRAVEL_TOO_SLOW, "6", 3);
    }

    /**
     * Check that the test feed has exactly one error for the given type, entityId, and entitySequence.
     */
    private void checkFeedHasError(NewGTFSErrorType type, String entityId, int entitySequence) {
        assertThatSqlCountQueryYieldsExpectedCount(
            testDataSource,
            String.format("select count(*) from %s.errors where error_type = '%s' and entity_id = '%s' and entity_sequence = %s",
                testNamespace,
                type,
                entityId,
                entitySequence),
                1);
    }

    /**
     * Check that the test feed is error free for the given type and entityId.
     */
    private void checkFeedIsErrorFree(NewGTFSErrorType type, String entityId) {
        assertThatSqlCountQueryYieldsExpectedCount(
                testDataSource,
                String.format("select count(*) from %s.errors where error_type = '%s' and entity_id = '%s'",
                        testNamespace,
                        type,
                        entityId),
                0);
    }
}
