package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.TestUtils;
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
import static com.conveyal.gtfs.TestUtils.loadFeedAndValidate;

/**
 * Load in a GTFS feed with GTFS flex features, and ensure all needed fields are imported correctly.
 * TODO: update feed to use more features, and test for these.
 */
public class GtfsFlexTest {
    private static String washingtonTestDBName;
    private static DataSource washingtonTestDataSource;
    private static String washingtonTestNamespace;
    private static String fakeAgencyTestDBName;
    private static DataSource fakeAgencyTestDataSource;
    private static String fakeAgencyTestNamespace;

    @BeforeAll
    public static void setUpClass() throws IOException {

        washingtonTestDBName = TestUtils.generateNewDB();
        washingtonTestDataSource = TestUtils.createTestDataSource(String.format("jdbc:postgresql://localhost/%s", washingtonTestDBName));
        washingtonTestNamespace = loadFeedAndValidate(washingtonTestDataSource, "real-world-gtfs-feeds/washington-park-shuttle-with-flex-additions");

        fakeAgencyTestDBName = TestUtils.generateNewDB();
        fakeAgencyTestDataSource = TestUtils.createTestDataSource(String.format("jdbc:postgresql://localhost/%s", fakeAgencyTestDBName));
        fakeAgencyTestNamespace = loadFeedAndValidate(fakeAgencyTestDataSource, "fake-agency-with-flex");
    }

    @AfterAll
    public static void tearDownClass() {
        TestUtils.dropDB(washingtonTestDBName);
        TestUtils.dropDB(fakeAgencyTestDBName);
    }

    @ParameterizedTest
    @MethodSource("createContinuousPickupAndDropOffChecks")
    void continuousPickupAndDropOffTests(String namespace, String field, int value, int expectedCount) {
        String query = String.format("select count(*) from %s.stop_times where %s = '%s'",
                namespace,
                field,
                value);
        assertThatSqlCountQueryYieldsExpectedCount(washingtonTestDataSource, query, expectedCount);
    }

    private static Stream<Arguments> createContinuousPickupAndDropOffChecks() {
        return Stream.of(
                Arguments.of(washingtonTestNamespace, "continuous_pickup", 0, 3),
                Arguments.of(washingtonTestNamespace, "continuous_pickup", 1, 5),
                Arguments.of(washingtonTestNamespace, "continuous_pickup", 2, 1),
                Arguments.of(washingtonTestNamespace, "continuous_drop_off", 0, 2),
                Arguments.of(washingtonTestNamespace, "continuous_drop_off", 1, 5),
                Arguments.of(washingtonTestNamespace, "continuous_drop_off", 2, 2)
        );
    }

    @Test
    void hasLoadedExpectedNumberOfBookingRules() {
        String query = buildQuery(fakeAgencyTestNamespace, "booking_rules","booking_rule_id","1");
        assertThatSqlCountQueryYieldsExpectedCount(fakeAgencyTestDataSource, query, 1);
    }

    @Test
    void hasLoadedExpectedNumberOfLocationGroups() {
        String query = buildQuery(fakeAgencyTestNamespace, "location_groups","location_group_id","1");
        assertThatSqlCountQueryYieldsExpectedCount(fakeAgencyTestDataSource, query, 1);
    }

    @Test
    void hasLoadedExpectedNumberOfStopTimes() {
        String query = buildQuery(fakeAgencyTestNamespace, "stop_times","pickup_booking_rule_id","1");
        assertThatSqlCountQueryYieldsExpectedCount(fakeAgencyTestDataSource, query, 5);
    }

    private String buildQuery(String namespace, String tableName, String columnName, String columnValue) {
        return String.format("select count(*) from %s.%s where %s = '%s'",
                namespace,
                tableName,
                columnName,
                columnValue);
    }
}
