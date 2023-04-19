package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.TestUtils;
import com.conveyal.gtfs.TestUtils.DataExpectation;
import com.conveyal.gtfs.TestUtils.FileTestCase;
import com.conveyal.gtfs.util.GeoJsonUtil;
import mil.nga.sf.geojson.Feature;
import mil.nga.sf.geojson.FeatureCollection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.conveyal.gtfs.TestUtils.assertThatSqlCountQueryYieldsExpectedCount;
import static com.conveyal.gtfs.TestUtils.loadFeedAndValidate;
import static com.conveyal.gtfs.TestUtils.lookThroughFiles;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Load in a GTFS feed with GTFS flex features, and ensure all needed fields are imported correctly.
 * TODO: update feed to use more features, and test for these.
 */
public class GtfsFlexTest {
    private static String washingtonTestDBName;
    private static DataSource washingtonTestDataSource;
    private static String washingtonTestNamespace;
    private static String doloresCountyTestDBName;
    private static DataSource doloresCountyTestDataSource;
    private static String doloresCountyTestNamespace;
    private static String doloresCountyGtfsZipFileName;
    private static String unexpectedGeoJsonZipFileName;

    @BeforeAll
    public static void setUpClass() throws IOException {
        washingtonTestDBName = TestUtils.generateNewDB();
        washingtonTestDataSource = TestUtils.createTestDataSource(String.format("jdbc:postgresql://localhost/%s", washingtonTestDBName));
        washingtonTestNamespace = loadFeedAndValidate(washingtonTestDataSource, "real-world-gtfs-feeds/washington-park-shuttle-with-flex-additions");

        doloresCountyTestDBName = TestUtils.generateNewDB();
        doloresCountyTestDataSource = TestUtils.createTestDataSource(String.format("jdbc:postgresql://localhost/%s", doloresCountyTestDBName));
        doloresCountyTestNamespace = loadFeedAndValidate(doloresCountyTestDataSource, "real-world-gtfs-feeds/dolorescounty-co-us--flex-v2");

        doloresCountyGtfsZipFileName = TestUtils.zipFolderFiles("real-world-gtfs-feeds/dolorescounty-co-us--flex-v2", true);
        unexpectedGeoJsonZipFileName = TestUtils.zipFolderFiles("fake-agency-unexpected-geojson", true);
    }

    @AfterAll
    public static void tearDownClass() {
        TestUtils.dropDB(washingtonTestDBName);
        TestUtils.dropDB(doloresCountyTestDBName);
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
        String query = buildQuery(doloresCountyTestNamespace, "booking_rules","booking_rule_id","booking_route_16604");
        assertThatSqlCountQueryYieldsExpectedCount(doloresCountyTestDataSource, query, 1);
    }

    @Test
    void hasLoadedExpectedNumberOfStopTimes() {
        String query = buildQuery(doloresCountyTestNamespace, "stop_times","pickup_booking_rule_id","booking_route_16604");
        assertThatSqlCountQueryYieldsExpectedCount(doloresCountyTestDataSource, query, 2);
    }

    @Test
    void hasLoadedExpectedNumberOfLocationGroups() {
        String query = buildQuery(doloresCountyTestNamespace, "location_groups","location_group_id","1");
        assertThatSqlCountQueryYieldsExpectedCount(doloresCountyTestDataSource, query, 1);
    }

    @Test
    void hasLoadedExpectedNumberOfLocations() {
        String query = buildQuery(doloresCountyTestNamespace, "locations","geometry_type","polygon");
        assertThatSqlCountQueryYieldsExpectedCount(doloresCountyTestDataSource, query, 2);
    }

    @Test
    void hasLoadedExpectedNumberOfPatternLocations() {
        String query = buildQuery(doloresCountyTestNamespace, "pattern_locations","pattern_id","1");
        assertThatSqlCountQueryYieldsExpectedCount(doloresCountyTestDataSource, query, 2);
    }

    @ParameterizedTest
    @MethodSource("createLocationShapeChecks")
    void hasLoadedExpectedNumberOfLocationShapes(String namespace, String field, String value, int expectedCount) {
        String query = String.format("select count(*) from %s.location_shapes where %s = '%s'",
            namespace,
            field,
            value);
        assertThatSqlCountQueryYieldsExpectedCount(doloresCountyTestDataSource, query, expectedCount);
    }

    private static Stream<Arguments> createLocationShapeChecks() {
        return Stream.of(
            Arguments.of(doloresCountyTestNamespace, "location_id", "area_275", 2037),
            Arguments.of(doloresCountyTestNamespace, "location_id", "area_276", 33)
        );
    }

    private String buildQuery(String namespace, String tableName, String columnName, String columnValue) {
        return String.format("select count(*) from %s.%s where %s = '%s'",
                namespace,
                tableName,
                columnName,
                columnValue);
    }

    /**
     * Make sure that unexpected geo json values are handled gracefully.
     */
    @Test
    void canHandleUnexpectedGeoJsonValues() {
        GTFSFeed feed = GTFSFeed.fromFile(unexpectedGeoJsonZipFileName);
        assertEquals("loc_1", feed.locations.entrySet().iterator().next().getKey());
        assertEquals("Plymouth Metrolink", feed.locations.values().iterator().next().stop_name);
        assertEquals("743", feed.locations.values().iterator().next().zone_id);
        assertEquals("http://www.test.com", feed.locations.values().iterator().next().stop_url.toString());
        assertNull(feed.locations.values().iterator().next().stop_desc);
    }

    /**
     * Make sure a round trip of loading a GTFS zip file and then writing another zip file can be performed with flex
     * data.
     */
    @Test
    public void canLoadAndWriteToFlexContentZipFile() throws IOException {
        // create a temp file for this test
        File outZip = File.createTempFile("dolorescounty-co-us--flex-v2", ".zip");
        GTFSFeed feed = GTFSFeed.fromFile(doloresCountyGtfsZipFileName);
        feed.toFile(outZip.getAbsolutePath());
        feed.close();
        assertThat(outZip.exists(), is(true));

        // assert that rows of data were written to files within the zipfile
        try (ZipFile zip = new ZipFile(outZip)) {
            ZipEntry entry = zip.getEntry("locations.geojson");
            FeatureCollection featureCollection = GeoJsonUtil.getLocations(zip, entry);
            List<Feature> features = featureCollection.getFeatures();
            assertEquals(features.get(0).getId(),"area_275");
            assertEquals(features.get(1).getId(),"area_276");

            FileTestCase[] fileTestCases = {
                // booking_rules.txt
                new FileTestCase(
                    "booking_rules.txt",
                    new DataExpectation[]{
                        new DataExpectation("booking_rule_id", "booking_route_16604"),
                        new DataExpectation("booking_type", "2"),
                        new DataExpectation("prior_notice_start_time", "08:00:00"),
                        new DataExpectation("prior_notice_last_time", "17:00:00")
                    }
                ),
                new TestUtils.FileTestCase(
                    "location_groups.txt",
                    new DataExpectation[]{
                        new DataExpectation("location_group_id", "1"),
                        new DataExpectation("location_id", "123"),
                        new DataExpectation("location_group_name", "This is the location group name")
                    }
                ),
                new TestUtils.FileTestCase(
                    "stop_times.txt",
                    new DataExpectation[]{
                        new DataExpectation("pickup_booking_rule_id", "booking_route_16604"),
                        new DataExpectation("drop_off_booking_rule_id", "booking_route_16604"),
                        new DataExpectation("start_pickup_dropoff_window", "08:00:00"),
                        new DataExpectation("end_pickup_dropoff_window", "17:00:00"),
                        new DataExpectation("mean_duration_factor", "1.0000000"),
                        new DataExpectation("mean_duration_offset", "15.0000000"),
                        new DataExpectation("safe_duration_factor", "1.0000000"),
                        new DataExpectation("safe_duration_offset", "20.0000000")
                    }
                ),
            };
            lookThroughFiles(fileTestCases, zip);
        }
        // delete file to make sure we can assert that this program created the file
        outZip.delete();
    }
}
