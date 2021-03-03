package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.TestUtils;
import com.conveyal.gtfs.loader.FeedLoadResult;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;

import static com.conveyal.gtfs.GTFS.load;
import static com.conveyal.gtfs.GTFS.validate;
import static com.conveyal.gtfs.TestUtils.assertThatSqlCountQueryYieldsExpectedCount;

public class SpeedTripValidatorTest {
    private static String testDBName;
    private static DataSource testDataSource;
    private static String testNamespace;
    private static final String TRAVEL_TOO_SLOW = "TRAVEL_TOO_SLOW";
    private static final String TRAVEL_TOO_FAST = "TRAVEL_TOO_FAST";

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
    public void canValidateNormalTravelSpeedWithAllStopTimes() throws Exception {
        checkFeedErrorsForExpectOutcome(TRAVEL_TOO_FAST, "1", 9, 0);
    }

    @Test
    public void canValidateNormalTravelSpeedWithMissingStopTimes() throws Exception {
        checkFeedErrorsForExpectOutcome(TRAVEL_TOO_FAST, "2", 3, 0);
    }

    @Test
    public void canValidateTooFastTravelSpeedWithAllStopTimes() throws Exception {
        checkFeedErrorsForExpectOutcome(TRAVEL_TOO_FAST, "3", 2, 1);
    }

    @Test
    public void canValidateTooFastTravelSpeedWithMissingStopTimes() throws Exception {
        checkFeedErrorsForExpectOutcome(TRAVEL_TOO_FAST, "4", 4, 1);
    }

    @Test
    public void canValidateTooSlowTravelSpeedWithAllStopTimes() throws Exception {
        checkFeedErrorsForExpectOutcome(TRAVEL_TOO_SLOW, "5", 3, 1);
    }

    @Test
    public void canValidateTooSlowTravelSpeedWithMissingStopTimes() throws Exception {
        checkFeedErrorsForExpectOutcome(TRAVEL_TOO_SLOW, "6", 3, 1);
    }

    private void checkFeedErrorsForExpectOutcome(String errorType,
                                                 String entityId,
                                                 int entitySequence,
                                                 int expectedCount) throws Exception {
        assertThatSqlCountQueryYieldsExpectedCount(
            testDataSource,
            String.format("select count(*) from %s.errors where error_type = '%s' and entity_id = '%s' and entity_sequence = %s",
                testNamespace,
                errorType,
                entityId,
                entitySequence),
            expectedCount);
    }
}
