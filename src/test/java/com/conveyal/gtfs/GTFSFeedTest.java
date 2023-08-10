package com.conveyal.gtfs;

import com.conveyal.gtfs.model.StopTime;

import org.hamcrest.comparator.ComparatorMatcherBuilder;
import com.conveyal.gtfs.TestUtils.DataExpectation;
import com.conveyal.gtfs.TestUtils.FileTestCase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.zip.ZipFile;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.number.IsCloseTo.closeTo;

/**
 * Test suite for the GTFSFeed class.
 */
public class GTFSFeedTest {

    private static final Logger LOG = LoggerFactory.getLogger(GTFSFeedTest.class);
    private static String simpleGtfsZipFileName;
    private static String simpleFlexGtfsZipFileName;

    @BeforeAll
    public static void setUpClass() {
        //executed only once, before the first test
        simpleGtfsZipFileName = null;
        simpleFlexGtfsZipFileName = null;
        try {
            simpleGtfsZipFileName = TestUtils.zipFolderFiles("fake-agency", true);
            simpleFlexGtfsZipFileName = TestUtils.zipFolderFiles("fake-agency-with-flex", true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Make sure a round-trip of loading a GTFS zip file and then writing another zip file can be performed.
     */
    @Test
    public void canDoRoundtripLoadAndWriteToZipFile() throws IOException {
        FileTestCase[] fileTestCases = {
            // agency.txt
            new FileTestCase(
                "agency.txt",
                new TestUtils.DataExpectation[]{
                    new TestUtils.DataExpectation("agency_id", "1"),
                    new TestUtils.DataExpectation("agency_name", "Fake Transit")
                }
            ),
            new FileTestCase(
                "calendar.txt",
                new DataExpectation[]{
                    new DataExpectation("service_id", "04100312-8fe1-46a5-a9f2-556f39478f57"),
                    new DataExpectation("start_date", "20170915"),
                    new DataExpectation("end_date", "20170917")
                }
            ),
            new FileTestCase(
                "routes.txt",
                new DataExpectation[]{
                    new DataExpectation("agency_id", "1"),
                    new DataExpectation("route_id", "1"),
                    new DataExpectation("route_long_name", "Route 1")
                }
            ),
            new FileTestCase(
                "shapes.txt",
                new DataExpectation[]{
                    new DataExpectation("shape_id", "5820f377-f947-4728-ac29-ac0102cbc34e"),
                    new DataExpectation("shape_pt_lat", "37.0612132"),
                    new DataExpectation("shape_pt_lon", "-122.0074332")
                }
            ),
            new FileTestCase(
                "stop_times.txt",
                new DataExpectation[]{
                    new DataExpectation("trip_id", "a30277f8-e50a-4a85-9141-b1e0da9d429d"),
                    new DataExpectation("departure_time", "07:00:00"),
                    new DataExpectation("stop_id", "4u6g")
                }
            ),
            new FileTestCase(
                "trips.txt",
                new DataExpectation[]{
                    new DataExpectation("route_id", "1"),
                    new DataExpectation("trip_id", "a30277f8-e50a-4a85-9141-b1e0da9d429d"),
                    new DataExpectation("service_id", "04100312-8fe1-46a5-a9f2-556f39478f57")
                }
            )
        };
        loadAndWriteToZipFile(simpleGtfsZipFileName, fileTestCases);

    }

    /**
     * Make sure a round-trip of loading a GTFS flex zip file and then writing another zip file can be performed.
     */
    @Test
    void canDoRoundTripLoadAndWriteToFlexZipFile() throws IOException {
        FileTestCase[] fileTestCases = {
            new FileTestCase(
                "agency.txt",
                new TestUtils.DataExpectation[]{
                    new TestUtils.DataExpectation("agency_id", "1"),
                    new TestUtils.DataExpectation("agency_name", "Fake Transit")
                }
            ),
            new FileTestCase(
                "areas.txt",
                new TestUtils.DataExpectation[]{
                    new TestUtils.DataExpectation("area_id", "1"),
                    new TestUtils.DataExpectation("area_name", "Area referencing a stop")
                }
            ),
            new FileTestCase(
                "booking_rules.txt",
                new TestUtils.DataExpectation[]{
                    new TestUtils.DataExpectation("booking_rule_id", "1"),
                    new TestUtils.DataExpectation("booking_type", "1"),
                    new TestUtils.DataExpectation("pickup_message", "This is a pickup message")
                }
            ),
            new FileTestCase(
                "calendar.txt",
                new DataExpectation[]{
                    new DataExpectation("service_id", "04100312-8fe1-46a5-a9f2-556f39478f57"),
                    new DataExpectation("start_date", "20170915"),
                    new DataExpectation("end_date", "20170917")
                }
            ),
            new FileTestCase(
                "routes.txt",
                new DataExpectation[]{
                    new DataExpectation("agency_id", "1"),
                    new DataExpectation("route_id", "1"),
                    new DataExpectation("route_long_name", "Route 1")
                }
            ),
            new FileTestCase(
                "shapes.txt",
                new DataExpectation[]{
                    new DataExpectation("shape_id", "5820f377-f947-4728-ac29-ac0102cbc34e"),
                    new DataExpectation("shape_pt_lat", "37.0612132"),
                    new DataExpectation("shape_pt_lon", "-122.0074332")
                }
            ),
            new FileTestCase(
                "stop_times.txt",
                new DataExpectation[]{
                    new DataExpectation("trip_id", "a30277f8-e50a-4a85-9141-b1e0da9d429d"),
                    new DataExpectation("departure_time", "07:00:00"),
                    new DataExpectation("stop_id", "4u6g")
                }
            ),
            new FileTestCase(
                "stop_areas.txt",
                new DataExpectation[]{
                    new DataExpectation("area_id", "2"),
                    new DataExpectation("stop_id", "area-999")
                }
            ),
            new FileTestCase(
                "trips.txt",
                new DataExpectation[]{
                    new DataExpectation("route_id", "1"),
                    new DataExpectation("trip_id", "a30277f8-e50a-4a85-9141-b1e0da9d429d"),
                    new DataExpectation("service_id", "04100312-8fe1-46a5-a9f2-556f39478f57")
                }
            )
        };
        loadAndWriteToZipFile(simpleFlexGtfsZipFileName, fileTestCases);
    }

    /**
     * Load feed and then write to zip file. Once complete, perform tests.
     */
    void loadAndWriteToZipFile(String zipFileName, FileTestCase[] fileTestCases) throws IOException {
        // create a temp file for this test
        File outZip = File.createTempFile(zipFileName, ".zip");

        // delete file to make sure we can assert that this program created the file
        outZip.delete();

        GTFSFeed feed = GTFSFeed.fromFile(zipFileName);
        feed.toFile(outZip.getAbsolutePath());
        feed.close();
        assertThat(outZip.exists(), is(true));

        // assert that rows of data were written to files within the zip file.
        ZipFile zip = new ZipFile(outZip);

        TestUtils.lookThroughFiles(fileTestCases, zip);
        // Close the zip file so it can be deleted.
        zip.close();
        // delete file to make sure we can assert that this program created the file
        outZip.delete();
    }

    /**
     * Make sure that a GTFS feed with interpolated stop times have calculated times after feed processing
     * @throws GTFSFeed.FirstAndLastStopsDoNotHaveTimes
     */
    @Test
    public void canGetInterpolatedTimes() throws GTFSFeed.FirstAndLastStopsDoNotHaveTimes, IOException {
        String tripId = "a30277f8-e50a-4a85-9141-b1e0da9d429d";

        String gtfsZipFileName = TestUtils.zipFolderFiles("fake-agency-interpolated-stop-times", true);

        GTFSFeed feed = GTFSFeed.fromFile(gtfsZipFileName);
        Iterable<StopTime> stopTimes = feed.getInterpolatedStopTimesForTrip(tripId);


        int i = 0;
        int lastStopSequence = -1;
        int lastDepartureTime = -1;
        for (StopTime st : stopTimes) {
            // assert that all stop times belong to same trip
            assertThat(st.trip_id, equalTo(tripId));

            // assert that stops in trip alternate
            if (i % 2 == 0) {
                assertThat(st.stop_id, equalTo("4u6g"));
            } else {
                assertThat(st.stop_id, equalTo("johv"));
            }

            // assert that sequence increases
            assertThat(
                st.stop_sequence,
                ComparatorMatcherBuilder.<Integer>usingNaturalOrdering().greaterThan(lastStopSequence)
            );
            lastStopSequence = st.stop_sequence;

            // assert that arrival and departure times are greater than the last ones
            assertThat(
                st.arrival_time,
                ComparatorMatcherBuilder.<Integer>usingNaturalOrdering().greaterThan(lastDepartureTime)
            );
            assertThat(
                st.departure_time,
                ComparatorMatcherBuilder.<Integer>usingNaturalOrdering().greaterThanOrEqualTo(st.arrival_time)
            );
            lastDepartureTime = st.departure_time;

            i++;
        }
    }

    /**
     * Make sure a spatial index of stops can be calculated
     */
    @Test
    public void canGetSpatialIndex() {
        GTFSFeed feed = GTFSFeed.fromFile(simpleGtfsZipFileName);
        assertThat(
            feed.getSpatialIndex().size(),
            // This should reflect the number of stops in src/test/resources/fake-agency/stops.txt
            equalTo(5)
        );
    }

    /**
     * Make sure trip speed can be calculated using trip's shape.
     */
    @Test
    public void canGetTripSpeedUsingShape() {
        GTFSFeed feed = GTFSFeed.fromFile(simpleGtfsZipFileName);
        assertThat(
            feed.getTripSpeed("a30277f8-e50a-4a85-9141-b1e0da9d429d"),
            is(closeTo(5.96, 0.01))
        );
    }

    /**
     * Make sure trip speed can be calculated using trip's shape.
     */
    @Test
    public void canGetTripSpeedUsingStraightLine() {
        GTFSFeed feed = GTFSFeed.fromFile(simpleGtfsZipFileName);
        assertThat(
            feed.getTripSpeed("a30277f8-e50a-4a85-9141-b1e0da9d429d", true),
            is(closeTo(5.18, 0.01))
        );
    }
}
