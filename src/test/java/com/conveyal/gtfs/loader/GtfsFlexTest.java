package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.TestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.IOException;

import static com.conveyal.gtfs.GTFS.load;
import static com.conveyal.gtfs.GTFS.validate;
import static com.conveyal.gtfs.TestUtils.assertThatSqlCountQueryYieldsExpectedCount;

/**
 * Load in a GTFS feed with GTFS flex features, and ensure all needed fields are imported correctly.
 * TODO: update feed to use more features, and test for these.
 */
public class GtfsFlexTest {
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
        String zipFileName = TestUtils.zipFolderFiles(
            "real-world-gtfs-feeds/washington-park-shuttle-with-flex-additions",
            true);
        FeedLoadResult feedLoadResult = load(zipFileName, testDataSource);
        testNamespace = feedLoadResult.uniqueIdentifier;
        validate(testNamespace, testDataSource);
    }

    @Test
    void canImportContinuousPickupAndDropoff() {
        String cp_0 = generateStopTimeQuery(
                testNamespace,
                "continuous_pickup",
                0);
        String cp_1 = generateStopTimeQuery(
                testNamespace,
                "continuous_pickup",
                1);
        String cp_2 = generateStopTimeQuery(
                testNamespace,
                "continuous_pickup",
                2);

        String cd_0 = generateStopTimeQuery(
                testNamespace,
                "continuous_drop_off",
                0);
        String cd_1 = generateStopTimeQuery(
                testNamespace,
                "continuous_drop_off",
                1);
        String cd_2 = generateStopTimeQuery(
                testNamespace,
                "continuous_drop_off",
                2);


        assertThatSqlCountQueryYieldsExpectedCount(testDataSource, cp_0, 3);
        assertThatSqlCountQueryYieldsExpectedCount(testDataSource, cp_1, 5);
        assertThatSqlCountQueryYieldsExpectedCount(testDataSource, cp_2, 1);
        assertThatSqlCountQueryYieldsExpectedCount(testDataSource, cd_0, 2);
        assertThatSqlCountQueryYieldsExpectedCount(testDataSource, cd_1, 5);
        assertThatSqlCountQueryYieldsExpectedCount(testDataSource, cd_2, 2);

    }

    private String generateStopTimeQuery(String namespace, String field, int value) {
        return String.format("select count(*) from %s.stop_times where %s = '%s'",
                testNamespace,
                field,
                value);
    }
}
