package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.TestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.stream.Stream;

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

    @ParameterizedTest
    @MethodSource("createContinuousPickupAndDropOffChecks")
    void continuousPickupAndDropOffTests(String namespace, String field, int value, int expectedCount) {
        String query = String.format("select count(*) from %s.stop_times where %s = '%s'",
                namespace,
                field,
                value);
        assertThatSqlCountQueryYieldsExpectedCount(testDataSource, query, expectedCount);
    }

    private static Stream<Arguments> createContinuousPickupAndDropOffChecks() {
        return Stream.of(
                Arguments.of(testNamespace, "continuous_pickup", 0, 3),
                Arguments.of(testNamespace, "continuous_pickup", 1, 5),
                Arguments.of(testNamespace, "continuous_pickup", 2, 1),
                Arguments.of(testNamespace, "continuous_drop_off", 0, 2),
                Arguments.of(testNamespace, "continuous_drop_off", 1, 5),
                Arguments.of(testNamespace, "continuous_drop_off", 2, 2)
        );
    }
}
