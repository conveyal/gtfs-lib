package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.TestUtils;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.loader.FeedLoadResult;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;

import static com.conveyal.gtfs.GTFS.load;
import static com.conveyal.gtfs.GTFS.validate;
import static com.conveyal.gtfs.TestUtils.assertThatSqlCountQueryYieldsExpectedCount;
import static com.conveyal.gtfs.error.NewGTFSErrorType.TRAVEL_TOO_FAST;
import static com.conveyal.gtfs.error.NewGTFSErrorType.TRAVEL_TOO_SLOW;

public class SpeedTripValidatorTest {
    private static String testDBName;
    private static DataSource testDataSource;
    private static String testNamespace;

    @BeforeClass
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

    @AfterClass
    public static void tearDownClass() {
        TestUtils.dropDB(testDBName);
    }

    @Test
    public void tripTravelingAtNormalSpeedWithAllStopTimesIsErrorFree() {
        checkFeedIsErrorFree(TRAVEL_TOO_FAST, "1");
        checkFeedIsErrorFree(TRAVEL_TOO_SLOW, "1");
    }

    @Test
    public void tripTravelingAtNormalSpeedWithMissingStopTimesIsErrorFree() {
        checkFeedIsErrorFree(TRAVEL_TOO_FAST, "2");
        checkFeedIsErrorFree(TRAVEL_TOO_SLOW, "2");
    }

    @Test
    public void tripTravelingTooFastWithAllStopTimesHasError() {
        checkFeedHasError(TRAVEL_TOO_FAST, "3", 2);
    }

    @Test
    public void tripTravelingTooFastWithMissingStopTimesHasError() {
        checkFeedHasError(TRAVEL_TOO_FAST, "4", 4);
    }

    @Test
    public void tripTravelingTooSlowWithAllStopTimesHasError() {
        checkFeedHasError(TRAVEL_TOO_SLOW, "5", 3);
    }

    @Test
    public void tripTravelingTooSlowWithMissingStopTimesHasError() {
        checkFeedHasError(TRAVEL_TOO_SLOW, "6", 3);
    }

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
