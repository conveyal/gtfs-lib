package com.conveyal.gtfs;


import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.loader.FeedLoadResult;
import com.conveyal.gtfs.loader.JdbcGtfsExporter;
import com.conveyal.gtfs.loader.SnapshotResult;
import com.conveyal.gtfs.storage.ErrorExpectation;
import com.conveyal.gtfs.storage.ExpectedFieldType;
import com.conveyal.gtfs.storage.PersistenceExpectation;
import com.conveyal.gtfs.storage.RecordExpectation;
import com.conveyal.gtfs.util.InvalidNamespaceException;
import com.conveyal.gtfs.validator.FeedValidatorCreator;
import com.conveyal.gtfs.validator.MTCValidator;
import com.conveyal.gtfs.validator.ValidationResult;
import com.csvreader.CsvReader;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import graphql.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.hamcrest.Matcher;
import org.hamcrest.comparator.ComparatorMatcherBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;


/**
 * A test suite for the {@link GTFS} Class.
 */
public class GTFSTest {
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private static final String JDBC_URL = "jdbc:postgresql://localhost";
    private static final Logger LOG = LoggerFactory.getLogger(GTFSTest.class);

    // setup a stream to capture the output from the program
    @BeforeEach
    public void setUpStreams() {
        System.setOut(new PrintStream(outContent));
    }

    /**
     * Make sure that help can be printed.
     *
     * @throws Exception
     */
    @Test
    public void canPrintHelp() throws Exception {
        String[] args = {"-help"};
        GTFS.main(args);
        assertThat(outContent.toString(), containsString("usage: java"));
    }

    /**
     * Make sure that help is printed if no recognizable arguments are provided.
     *
     * @throws Exception
     */
    @Test
    public void handlesUnknownArgs() throws Exception {
        String[] args = {"-blah"};
        GTFS.main(args);
        assertThat(outContent.toString(), containsString("usage: java"));
    }

    /**
     * Make sure that help is printed if not enough key arguments are provided.
     *
     * @throws Exception
     */
    @Test
    public void requiresActionCommand() throws Exception {
        String[] args = {"-u", "blah"};
        GTFS.main(args);
        assertThat(outContent.toString(), containsString("usage: java"));
    }

    /**
     * Tests whether or not a super simple 2-stop, 1-route, 1-trip, valid gtfs can be loaded and exported
     */
    @Test
    public void canLoadAndExportSimpleAgency() {
        ErrorExpectation[] fakeAgencyErrorExpectations = ErrorExpectation.list(
            new ErrorExpectation(NewGTFSErrorType.MISSING_FIELD),
            new ErrorExpectation(NewGTFSErrorType.REFERENTIAL_INTEGRITY),
            new ErrorExpectation(NewGTFSErrorType.ROUTE_LONG_NAME_CONTAINS_SHORT_NAME),
            new ErrorExpectation(NewGTFSErrorType.FEED_TRAVEL_TIMES_ROUNDED),
            new ErrorExpectation(NewGTFSErrorType.STOP_UNUSED, equalTo("1234567")),
            new ErrorExpectation(NewGTFSErrorType.DATE_NO_SERVICE)
        );
        assertThat(
            runIntegrationTestOnFolder(
                "fake-agency",
                nullValue(),
                fakeAgencyPersistenceExpectations,
                fakeAgencyErrorExpectations
            ),
            equalTo(true)
        );
    }

    /**
     * Tests that a GTFS feed with bad date values in calendars.txt and calendar_dates.txt can pass the integration test.
     */
    @Test
    public void canLoadFeedWithBadDates () {
        PersistenceExpectation[] expectations = PersistenceExpectation.list(
            new PersistenceExpectation(
                "calendar",
                new RecordExpectation[]{
                    new RecordExpectation("start_date", null)
                }
            )
        );
        ErrorExpectation[] errorExpectations = ErrorExpectation.list(
            new ErrorExpectation(NewGTFSErrorType.MISSING_FIELD),
            new ErrorExpectation(NewGTFSErrorType.DATE_FORMAT),
            new ErrorExpectation(NewGTFSErrorType.DATE_FORMAT),
            new ErrorExpectation(NewGTFSErrorType.DATE_FORMAT),
            new ErrorExpectation(NewGTFSErrorType.REFERENTIAL_INTEGRITY),
            new ErrorExpectation(NewGTFSErrorType.DATE_FORMAT),
            new ErrorExpectation(NewGTFSErrorType.DATE_FORMAT),
            // The below "wrong number of fields" errors are for empty new lines
            // found in the file.
            new ErrorExpectation(NewGTFSErrorType.WRONG_NUMBER_OF_FIELDS),
            new ErrorExpectation(NewGTFSErrorType.WRONG_NUMBER_OF_FIELDS),
            new ErrorExpectation(NewGTFSErrorType.WRONG_NUMBER_OF_FIELDS),
            new ErrorExpectation(NewGTFSErrorType.WRONG_NUMBER_OF_FIELDS),
            new ErrorExpectation(NewGTFSErrorType.WRONG_NUMBER_OF_FIELDS),
            new ErrorExpectation(NewGTFSErrorType.REFERENTIAL_INTEGRITY),
            new ErrorExpectation(NewGTFSErrorType.ROUTE_LONG_NAME_CONTAINS_SHORT_NAME),
            new ErrorExpectation(NewGTFSErrorType.FEED_TRAVEL_TIMES_ROUNDED),
            new ErrorExpectation(NewGTFSErrorType.SERVICE_NEVER_ACTIVE),
            new ErrorExpectation(NewGTFSErrorType.TRIP_NEVER_ACTIVE),
            new ErrorExpectation(NewGTFSErrorType.SERVICE_UNUSED),
            new ErrorExpectation(NewGTFSErrorType.DATE_NO_SERVICE)
        );
        assertThat(
            "Integration test passes",
            runIntegrationTestOnFolder("fake-agency-bad-calendar-date", nullValue(), expectations, errorExpectations),
            equalTo(true)
        );
    }

    /**
     * Tests that a GTFS feed with blank (unspecified) values for pickup and dropoff types in stop_times.txt
     * is loaded with the blank values resolved, so that the patterns are counted correctly.
     */
    @Test
    public void canLoadFeedAndResolveUnsetPickupDropOffValues () {
        PersistenceExpectation persistenceExpectation1 = makePickupDropOffPersistenceExpectation(1);
        PersistenceExpectation persistenceExpectation2 = makePickupDropOffPersistenceExpectation(2);

        PersistenceExpectation[] expectations1 = PersistenceExpectation.list(
            persistenceExpectation1
        );
        PersistenceExpectation[] expectations2 = PersistenceExpectation.list(
            persistenceExpectation1,
            // There should be only one pattern, so the record below should not be created after loading the test feed.
            persistenceExpectation2
        );
        ErrorExpectation[] errorExpectations = ErrorExpectation.list(
            new ErrorExpectation(NewGTFSErrorType.FEED_TRAVEL_TIMES_ROUNDED)
        );

        // The first pattern should be added to the editor patterns table.
        assertThat(
            "There should be one pattern in the patterns table after resolving blank pickup/dropoff values in stop_times.",
            runIntegrationTestOnFolder("fake-ferry-blank-pickups", nullValue(), expectations1, errorExpectations),
            equalTo(true)
        );

        // The second pattern should not be added to the editor patterns table
        // (there *should* be an assertion error about the second record from expectations2 not found).
        assertThrows(
            AssertionError.class,
            () -> runIntegrationTestOnFolder("fake-ferry-blank-pickups", nullValue(), expectations2, errorExpectations),
            "There should be *only* one pattern in the patterns table after resolving blank pickup/dropoff values."
        );
    }

    private PersistenceExpectation makePickupDropOffPersistenceExpectation(int index) {
         return new PersistenceExpectation(
            "patterns",
            new RecordExpectation[]{
                new RecordExpectation("pattern_id", String.valueOf(index)),
                new RecordExpectation("route_id", "Tib-AIF"),
                new RecordExpectation("direction_id", 1),
                new RecordExpectation("shape_id", "y7d8"),
            },
            true
        );
    }

    /**
     * Tests that a GTFS feed with errors is loaded properly and that the various errors were detected and stored in the
     * database.
     */
    @Test
    public void canLoadFeedWithErrors () {
        PersistenceExpectation[] expectations = PersistenceExpectation.list();
        ErrorExpectation[] errorExpectations = ErrorExpectation.list(
            new ErrorExpectation(NewGTFSErrorType.FARE_TRANSFER_MISMATCH, equalTo("fare-02")),
            new ErrorExpectation(NewGTFSErrorType.FREQUENCY_PERIOD_OVERLAP, equalTo("freq-01_08:30:00_to_10:15:00_every_15m00s")),
            new ErrorExpectation(NewGTFSErrorType.FREQUENCY_PERIOD_OVERLAP, equalTo("freq-01_08:30:00_to_10:15:00_every_15m00s")),
            new ErrorExpectation(NewGTFSErrorType.FREQUENCY_PERIOD_OVERLAP),
            new ErrorExpectation(NewGTFSErrorType.FREQUENCY_PERIOD_OVERLAP),
            new ErrorExpectation(NewGTFSErrorType.FREQUENCY_PERIOD_OVERLAP),
            new ErrorExpectation(NewGTFSErrorType.FREQUENCY_PERIOD_OVERLAP),
            new ErrorExpectation(NewGTFSErrorType.TRIP_OVERLAP_IN_BLOCK, equalTo("1A00000"))
        );
        assertThat(
            "Integration test passes",
            runIntegrationTestOnFolder("fake-agency-overlapping-trips", nullValue(), expectations, errorExpectations),
            equalTo(true)
        );
    }

    /**
     * Tests whether or not "fake-agency" GTFS can be placed in a zipped subdirectory and loaded/exported successfully.
     */
    @Test
    public void canLoadAndExportSimpleAgencyInSubDirectory() throws IOException {
        String zipFileName = null;
        // Get filename for fake-agency resource
        String resourceFolder = TestUtils.getResourceFileName("fake-agency");
        // Recursively copy folder into temp directory, which we zip up and run the integration test on.
        File tempDir = Files.createTempDirectory("").toFile();
        tempDir.deleteOnExit();
        File nestedDir = new File(TestUtils.fileNameWithDir(tempDir.getAbsolutePath(), "fake-agency"));
        LOG.info("Creating temp folder with nested subdirectory at {}", tempDir.getAbsolutePath());
        try {
            FileUtils.copyDirectory(new File(resourceFolder), nestedDir);
            zipFileName = TestUtils.zipFolderFiles(tempDir.getAbsolutePath(), false);
        } catch (IOException e) {
            e.printStackTrace();
        }
        ErrorExpectation[] errorExpectations = ErrorExpectation.list(
            new ErrorExpectation(NewGTFSErrorType.TABLE_IN_SUBDIRECTORY),
            new ErrorExpectation(NewGTFSErrorType.TABLE_IN_SUBDIRECTORY),
            new ErrorExpectation(NewGTFSErrorType.MISSING_FIELD),
            new ErrorExpectation(NewGTFSErrorType.TABLE_IN_SUBDIRECTORY),
            new ErrorExpectation(NewGTFSErrorType.TABLE_IN_SUBDIRECTORY),
            new ErrorExpectation(NewGTFSErrorType.TABLE_IN_SUBDIRECTORY),
            new ErrorExpectation(NewGTFSErrorType.TABLE_IN_SUBDIRECTORY),
            new ErrorExpectation(NewGTFSErrorType.TABLE_IN_SUBDIRECTORY),
            new ErrorExpectation(NewGTFSErrorType.TABLE_IN_SUBDIRECTORY),
            new ErrorExpectation(NewGTFSErrorType.TABLE_IN_SUBDIRECTORY),
            new ErrorExpectation(NewGTFSErrorType.TABLE_IN_SUBDIRECTORY),
            new ErrorExpectation(NewGTFSErrorType.TABLE_IN_SUBDIRECTORY),
            new ErrorExpectation(NewGTFSErrorType.TABLE_IN_SUBDIRECTORY),
            new ErrorExpectation(NewGTFSErrorType.TABLE_IN_SUBDIRECTORY),
            new ErrorExpectation(NewGTFSErrorType.TABLE_IN_SUBDIRECTORY),
            new ErrorExpectation(NewGTFSErrorType.TABLE_IN_SUBDIRECTORY),
            new ErrorExpectation(NewGTFSErrorType.TABLE_IN_SUBDIRECTORY),
            new ErrorExpectation(NewGTFSErrorType.REFERENTIAL_INTEGRITY),
            new ErrorExpectation(NewGTFSErrorType.ROUTE_LONG_NAME_CONTAINS_SHORT_NAME),
            new ErrorExpectation(NewGTFSErrorType.FEED_TRAVEL_TIMES_ROUNDED),
            new ErrorExpectation(NewGTFSErrorType.STOP_UNUSED),
            new ErrorExpectation(NewGTFSErrorType.DATE_NO_SERVICE)
        );
        assertThat(
            runIntegrationTestOnZipFile(zipFileName, nullValue(), fakeAgencyPersistenceExpectations, errorExpectations),
            equalTo(true)
        );
    }

    /**
     * Tests whether the simple gtfs can be loaded and exported if it has only calendar_dates.txt
     */
    @Test
    public void canLoadAndExportSimpleAgencyWithOnlyCalendarDates() {
        PersistenceExpectation[] persistenceExpectations = new PersistenceExpectation[]{
            new PersistenceExpectation(
                "agency",
                new RecordExpectation[]{
                    new RecordExpectation("agency_id", "1"),
                    new RecordExpectation("agency_name", "Fake Transit"),
                    new RecordExpectation("agency_timezone", "America/Los_Angeles")
                }
            ),
            new PersistenceExpectation(
                "calendar_dates",
                new RecordExpectation[]{
                    new RecordExpectation(
                        "service_id", "04100312-8fe1-46a5-a9f2-556f39478f57"
                    ),
                    new RecordExpectation("date", 20170916),
                    new RecordExpectation("exception_type", 1)
                }
            ),
            new PersistenceExpectation(
                "stop_times",
                new RecordExpectation[]{
                    new RecordExpectation(
                        "trip_id", "a30277f8-e50a-4a85-9141-b1e0da9d429d"
                    ),
                    new RecordExpectation("arrival_time", 25200, "07:00:00"),
                    new RecordExpectation("departure_time", 25200, "07:00:00"),
                    new RecordExpectation("stop_id", "4u6g"),
                    new RecordExpectation("stop_sequence", 1),
                    new RecordExpectation("pickup_type", 0),
                    new RecordExpectation("drop_off_type", 0),
                    new RecordExpectation("shape_dist_traveled", 0.0, 0.01)
                }
            ),
            // Check that the shape_dist_traveled values in stop_times are not rounded.
            new PersistenceExpectation(
                "stop_times",
                new RecordExpectation[]{
                    new RecordExpectation("shape_dist_traveled", 341.4491961, 0.00001)
                }
            ),
            new PersistenceExpectation(
                "trips",
                new RecordExpectation[]{
                    new RecordExpectation(
                        "trip_id", "a30277f8-e50a-4a85-9141-b1e0da9d429d"
                    ),
                    new RecordExpectation(
                        "service_id", "04100312-8fe1-46a5-a9f2-556f39478f57"
                    ),
                    new RecordExpectation("route_id", "1"),
                    new RecordExpectation("direction_id", 0),
                    new RecordExpectation(
                        "shape_id", "5820f377-f947-4728-ac29-ac0102cbc34e"
                    ),
                    new RecordExpectation("bikes_allowed", 0),
                    new RecordExpectation("wheelchair_accessible", 0)
                }
            )
        };
        ErrorExpectation[] errorExpectations = ErrorExpectation.list(
            new ErrorExpectation(NewGTFSErrorType.MISSING_FIELD),
            new ErrorExpectation(NewGTFSErrorType.ROUTE_LONG_NAME_CONTAINS_SHORT_NAME),
            new ErrorExpectation(NewGTFSErrorType.FEED_TRAVEL_TIMES_ROUNDED)
        );
        assertThat(
            runIntegrationTestOnFolder(
                "fake-agency-only-calendar-dates",
                nullValue(),
                persistenceExpectations,
                errorExpectations
            ),
            equalTo(true)
        );
    }

    /**
     * Tests whether the simple gtfs can be loaded and exported if it has a mixture of service_id definitions in both
     * the calendar.txt and calendar_dates.txt files.
     */@Test
    public void canLoadAndExportSimpleAgencyWithMixtureOfCalendarDefinitions() {
        PersistenceExpectation[] persistenceExpectations = new PersistenceExpectation[]{
            new PersistenceExpectation(
                "agency",
                new RecordExpectation[]{
                    new RecordExpectation("agency_id", "1"),
                    new RecordExpectation("agency_name", "Fake Transit"),
                    new RecordExpectation("agency_timezone", "America/Los_Angeles")
                }
            ),
            // calendar.txt-only expectation
            new PersistenceExpectation(
                "calendar",
                new RecordExpectation[]{
                    new RecordExpectation("service_id", "only-in-calendar-txt"),
                    new RecordExpectation("start_date", 20170915),
                    new RecordExpectation("end_date", 20170917)
                }
            ),
            // calendar.txt and calendar-dates.txt expectation
            new PersistenceExpectation(
                "calendar",
                new RecordExpectation[]{
                    new RecordExpectation("service_id", "in-both-calendar-txt-and-calendar-dates"),
                    new RecordExpectation("start_date", 20170918),
                    new RecordExpectation("end_date", 20170920)
                }
            ),
            new PersistenceExpectation(
                "calendar_dates",
                new RecordExpectation[]{
                    new RecordExpectation(
                        "service_id", "in-both-calendar-txt-and-calendar-dates"
                    ),
                    new RecordExpectation("date", 20170920),
                    new RecordExpectation("exception_type", 2)
                }
            ),
            // calendar-dates.txt-only expectation
            new PersistenceExpectation(
                "calendar",
                new RecordExpectation[]{
                    new RecordExpectation(
                        "service_id", "only-in-calendar-dates-txt"
                    ),
                    new RecordExpectation("start_date", 20170916),
                    new RecordExpectation("end_date", 20170916)
                },
                true
            ),
            new PersistenceExpectation(
                "calendar_dates",
                new RecordExpectation[]{
                    new RecordExpectation(
                        "service_id", "only-in-calendar-dates-txt"
                    ),
                    new RecordExpectation("date", 20170916),
                    new RecordExpectation("exception_type", 1)
                }
            ),
            new PersistenceExpectation(
                "stop_times",
                new RecordExpectation[]{
                    new RecordExpectation(
                        "trip_id", "non-frequency-trip"
                    ),
                    new RecordExpectation("arrival_time", 25200, "07:00:00"),
                    new RecordExpectation("departure_time", 25200, "07:00:00"),
                    new RecordExpectation("stop_id", "4u6g"),
                    new RecordExpectation("stop_sequence", 1),
                    new RecordExpectation("pickup_type", 0),
                    new RecordExpectation("drop_off_type", 0),
                    new RecordExpectation("shape_dist_traveled", 0.0, 0.01)
                }
            ),
            // calendar-dates only expectation
            new PersistenceExpectation(
                "trips",
                new RecordExpectation[]{
                    new RecordExpectation(
                        "trip_id", "non-frequency-trip"
                    ),
                    new RecordExpectation(
                        "service_id", "only-in-calendar-dates-txt"
                    ),
                    new RecordExpectation("route_id", "1"),
                    new RecordExpectation("direction_id", 0),
                    new RecordExpectation(
                        "shape_id", "5820f377-f947-4728-ac29-ac0102cbc34e"
                    ),
                    new RecordExpectation("bikes_allowed", 0),
                    new RecordExpectation("wheelchair_accessible", 0)
                }
            ),
            // calendar-only expectation
            new PersistenceExpectation(
                "trips",
                new RecordExpectation[]{
                    new RecordExpectation(
                        "trip_id", "non-frequency-trip-2"
                    ),
                    new RecordExpectation(
                        "service_id", "only-in-calendar-txt"
                    ),
                    new RecordExpectation("route_id", "1"),
                    new RecordExpectation("direction_id", 0),
                    new RecordExpectation(
                        "shape_id", "5820f377-f947-4728-ac29-ac0102cbc34e"
                    ),
                    new RecordExpectation("bikes_allowed", 0),
                    new RecordExpectation("wheelchair_accessible", 0)
                }
            ),
            // calendar-dates and calendar expectation
            new PersistenceExpectation(
                "trips",
                new RecordExpectation[]{
                    new RecordExpectation(
                        "trip_id", "frequency-trip"
                    ),
                    new RecordExpectation(
                        "service_id", "in-both-calendar-txt-and-calendar-dates"
                    ),
                    new RecordExpectation("route_id", "1"),
                    new RecordExpectation("direction_id", 0),
                    new RecordExpectation(
                        "shape_id", "5820f377-f947-4728-ac29-ac0102cbc34e"
                    ),
                    new RecordExpectation("bikes_allowed", 0),
                    new RecordExpectation("wheelchair_accessible", 0)
                }
            )
        };
        ErrorExpectation[] errorExpectations = ErrorExpectation.list(
            new ErrorExpectation(NewGTFSErrorType.MISSING_FIELD),
            new ErrorExpectation(NewGTFSErrorType.ROUTE_LONG_NAME_CONTAINS_SHORT_NAME),
            new ErrorExpectation(NewGTFSErrorType.FEED_TRAVEL_TIMES_ROUNDED)
        );
        assertThat(
            runIntegrationTestOnFolder(
                "fake-agency-mixture-of-calendar-definitions",
                nullValue(),
                persistenceExpectations,
                errorExpectations
            ),
            equalTo(true)
        );
    }

    /**
     * Tests that a GTFS feed with long field values generates corresponding
     * validation errors per MTC guidelines.
     */
    @Test
    public void canLoadFeedWithLongFieldValues () {
        PersistenceExpectation[] expectations = PersistenceExpectation.list();
        ErrorExpectation[] errorExpectations = ErrorExpectation.list(
            new ErrorExpectation(NewGTFSErrorType.FIELD_VALUE_TOO_LONG),
            new ErrorExpectation(NewGTFSErrorType.FIELD_VALUE_TOO_LONG),
            new ErrorExpectation(NewGTFSErrorType.FIELD_VALUE_TOO_LONG),
            new ErrorExpectation(NewGTFSErrorType.FIELD_VALUE_TOO_LONG),
            new ErrorExpectation(NewGTFSErrorType.FIELD_VALUE_TOO_LONG),
            new ErrorExpectation(NewGTFSErrorType.FIELD_VALUE_TOO_LONG),
            new ErrorExpectation(NewGTFSErrorType.FEED_TRAVEL_TIMES_ROUNDED) // Not related, not worrying about this one.
        );
        assertThat(
            "Long-field-value test passes",
            runIntegrationTestOnFolder(
                "fake-agency-mtc-long-fields",
                nullValue(),
                expectations,
                errorExpectations,
                MTCValidator::new
            ),
            equalTo(true)
        );
    }

    /**
     * Tests that a GTFS feed with a service id that doesn't apply to any day of the week
     * (i.e. when 'monday' through 'sunday' fields are set to zero)
     * generates a validation error.
     */
    @Test
    public void canLoadFeedWithServiceWithoutDaysOfWeek() {
        PersistenceExpectation[] expectations = PersistenceExpectation.list();
        ErrorExpectation[] errorExpectations = ErrorExpectation.list(
            new ErrorExpectation(NewGTFSErrorType.FEED_TRAVEL_TIMES_ROUNDED), // Not related, not worrying about this one.
            new ErrorExpectation(NewGTFSErrorType.SERVICE_WITHOUT_DAYS_OF_WEEK)
        );
        assertThat(
            "service-without-days test passes",
            runIntegrationTestOnFolder(
                "fake-agency-service-without-days",
                nullValue(),
                expectations,
                errorExpectations
            ),
            equalTo(true)
        );
    }

    /**
     * A helper method that will zip a specified folder in test/main/resources and call
     * {@link #runIntegrationTestOnZipFile} on that file.
     */
    private boolean runIntegrationTestOnFolder(
        String folderName,
        Matcher<Object> fatalExceptionExpectation,
        PersistenceExpectation[] persistenceExpectations,
        ErrorExpectation[] errorExpectations,
        FeedValidatorCreator... customValidators
    ) {
        LOG.info("Running integration test on folder {}", folderName);
        // zip up test folder into temp zip file
        String zipFileName = null;
        try {
            zipFileName = TestUtils.zipFolderFiles(folderName, true);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return runIntegrationTestOnZipFile(
            zipFileName,
            fatalExceptionExpectation,
            persistenceExpectations,
            errorExpectations,
            customValidators
        );
    }

    /**
     * Load a feed and remove all data from the tables that match the mandatory file list. Confirm that the export
     * contains the mandatory files which will be exported even though the matching table has no data.
     */
    @Test
    void canExportEmptyMandatoryFiles() {
        String testDBName = TestUtils.generateNewDB();
        File tempFile = null;
        try {
            String zipFileName = TestUtils.zipFolderFiles("fake-agency", true);
            String dbConnectionUrl = String.join("/", JDBC_URL, testDBName);
            DataSource dataSource = TestUtils.createTestDataSource(dbConnectionUrl);
            FeedLoadResult loadResult = GTFS.load(zipFileName, dataSource);
            String namespace = loadResult.uniqueIdentifier;

            // Remove data from tables that match the mandatory files.
            for (String fileName : JdbcGtfsExporter.mandatoryFileList) {
                try (Connection connection = dataSource.getConnection()) {
                    String tableName = fileName.split("\\.")[0];
                    String sql = String.format("delete from %s.%s", namespace, tableName);
                    LOG.info(sql);
                    connection.prepareStatement(sql).execute();
                }
            }

            // Confirm that the mandatory files are present in the zip file.
            tempFile = exportGtfs(namespace, dataSource, false);
            ZipFile gtfsZipFile = new ZipFile(tempFile.getAbsolutePath());
            for (String fileName : JdbcGtfsExporter.mandatoryFileList) {
                Assert.assertNotNull(gtfsZipFile.getEntry(fileName));
            }
        } catch (IOException | SQLException e) {
            LOG.error("An error occurred while attempting to test exporting of mandatory files.", e);
        } finally {
            TestUtils.dropDB(testDBName);
            if (tempFile != null) tempFile.deleteOnExit();
        }
    }

    /**
     * A helper method that will run GTFS#main with a certain zip file.
     * This tests whether a GTFS zip file can be loaded without any errors. The full list of steps includes:
     * 1. GTFS#load
     * 2. GTFS#validate
     * 3. exportGtfs/check exported GTFS integrity
     * 4. makeSnapshot
     * 5. Delete feed/namespace
     */
    private boolean runIntegrationTestOnZipFile(
        String zipFileName,
        Matcher<Object> fatalExceptionExpectation,
        PersistenceExpectation[] persistenceExpectations,
        ErrorExpectation[] errorExpectations,
        FeedValidatorCreator... customValidators
    ) {
        String testDBName = TestUtils.generateNewDB();
        String dbConnectionUrl = String.join("/", JDBC_URL, testDBName);
        DataSource dataSource = TestUtils.createTestDataSource(dbConnectionUrl);

        String namespace;

        // Verify that loading the feed completes and data is stored properly
        try (Connection connection = dataSource.getConnection()) {
            // load and validate feed
            LOG.info("load and validate GTFS file {}", zipFileName);
            FeedLoadResult loadResult = GTFS.load(zipFileName, dataSource);
            ValidationResult validationResult = GTFS.validate(
                loadResult.uniqueIdentifier,
                dataSource,
                customValidators
            );

            assertThat(validationResult.fatalException, is(fatalExceptionExpectation));
            namespace = loadResult.uniqueIdentifier;
            assertThatImportedGtfsMeetsExpectations(
                connection,
                namespace,
                persistenceExpectations,
                errorExpectations,
                false
            );

            // Verify that exporting the feed (in non-editor mode) completes and data is outputted properly
            LOG.info("export GTFS from created namespace");
            File tempFile = exportGtfs(namespace, dataSource, false);
            assertThatExportedGtfsMeetsExpectations(tempFile, persistenceExpectations, false);

            // Verify that making a snapshot from an existing feed database, then exporting that snapshot to a GTFS zip
            // file works as expected
            boolean snapshotIsOk = assertThatSnapshotIsSuccessful(
                connection,
                namespace,
                dataSource,
                testDBName,
                persistenceExpectations,
                false
            );
            if (!snapshotIsOk) return false;
            // Also, verify that if we're normalizing stop_times#stop_sequence, the stop_sequence values conform with
            // our expectations (zero-based, incrementing values).
            PersistenceExpectation[] expectationsWithNormalizedStopTimesSequence =
                updatePersistenceExpectationsWithNormalizedStopTimesSequence(persistenceExpectations);
            boolean normalizedSnapshotIsOk = assertThatSnapshotIsSuccessful(
                connection,
                namespace,
                dataSource,
                testDBName,
                expectationsWithNormalizedStopTimesSequence,
                true
            );
            if (!normalizedSnapshotIsOk) return false;
        } catch (IOException | SQLException e) {
            LOG.error("An error occurred while loading/snapshotting the database!");
            TestUtils.dropDB(testDBName);
            e.printStackTrace();
            return false;
        } catch (AssertionError e) {
            TestUtils.dropDB(testDBName);
            throw e;
        }

        // Get a new connection here, because sometimes the old connection causes hanging issues upon trying to drop a
        // schema (via deleting a GTFS namespace).
        try (Connection connection = dataSource.getConnection()) {
            // Verify that deleting a feed works as expected.
            LOG.info("Deleting GTFS feed from database.");
            GTFS.delete(namespace, dataSource);

            String sql = String.format("select * from feeds where namespace = '%s'", namespace);
            LOG.info(sql);
            ResultSet resultSet = connection.prepareStatement(sql).executeQuery();
            while (resultSet.next()) {
                // Assert that the feed registry shows feed as deleted.
                assertThat(resultSet.getBoolean("deleted"), is(true));
            }
            // Ensure that schema no longer exists for namespace (note: this is Postgres specific).
            String schemaSql = String.format(
                "SELECT * FROM information_schema.schemata where schema_name = '%s'",
                namespace
            );
            LOG.info(schemaSql);
            ResultSet schemaResultSet = connection.prepareStatement(schemaSql).executeQuery();
            int schemaCount = 0;
            while (schemaResultSet.next()) schemaCount++;
            // There should be no schema records matching the deleted namespace.
            assertThat(schemaCount, is(0));
        } catch (SQLException | InvalidNamespaceException e) {
            LOG.error("An error occurred while deleting a schema!", e);
            TestUtils.dropDB(testDBName);
            return false;
        } catch (AssertionError e) {
            TestUtils.dropDB(testDBName);
            throw e;
        }

        // This should be run following all of the above tests (any new tests should go above these lines).
        TestUtils.dropDB(testDBName);
        return true;
    }

    private void assertThatLoadIsErrorFree(FeedLoadResult loadResult) {
        assertThat(loadResult.fatalException, is(nullValue()));
        assertThat(loadResult.agency.fatalException, is(nullValue()));
        assertThat(loadResult.calendar.fatalException, is(nullValue()));
        assertThat(loadResult.calendarDates.fatalException, is(nullValue()));
        assertThat(loadResult.fareAttributes.fatalException, is(nullValue()));
        assertThat(loadResult.fareRules.fatalException, is(nullValue()));
        assertThat(loadResult.feedInfo.fatalException, is(nullValue()));
        assertThat(loadResult.frequencies.fatalException, is(nullValue()));
        assertThat(loadResult.routes.fatalException, is(nullValue()));
        assertThat(loadResult.shapes.fatalException, is(nullValue()));
        assertThat(loadResult.stops.fatalException, is(nullValue()));
        assertThat(loadResult.stopTimes.fatalException, is(nullValue()));
        assertThat(loadResult.transfers.fatalException, is(nullValue()));
        assertThat(loadResult.trips.fatalException, is(nullValue()));
    }

    private void assertThatSnapshotIsErrorFree(SnapshotResult snapshotResult) {
        assertThatLoadIsErrorFree(snapshotResult);
        assertThat(snapshotResult.scheduleExceptions.fatalException, is(nullValue()));
    }

    /**
     * Helper function to export a GTFS from the database to a temporary zip file.
     */
    private File exportGtfs(String namespace, DataSource dataSource, boolean fromEditor) throws IOException {
        File tempFile = File.createTempFile("snapshot", ".zip");
        GTFS.export(namespace, tempFile.getAbsolutePath(), dataSource, fromEditor);
        return tempFile;
    }

    private class ValuePair {
        private final Object expected;
        private final Object found;
        private ValuePair (Object expected, Object found) {
            this.expected = expected;
            this.found = found;
        }
    }

    /**
     * Creates a snapshot, and asserts persistence expectations on the newly-created database of that snapshot. Then,
     * exports that snapshot to a GTFS and asserts persistence expectations on the newly-exported GTFS.
     */
    private boolean assertThatSnapshotIsSuccessful(
        Connection connection,
        String namespace,
        DataSource dataSource,
        String testDBName,
        PersistenceExpectation[] persistenceExpectations,
        boolean normalizeStopTimes
    ) {
        try {
            LOG.info("copy GTFS from created namespace");
            SnapshotResult copyResult = GTFS.makeSnapshot(namespace, dataSource, normalizeStopTimes);
            assertThatSnapshotIsErrorFree(copyResult);
            assertThatImportedGtfsMeetsExpectations(
                connection,
                copyResult.uniqueIdentifier,
                persistenceExpectations,
                null,
                true
            );
            LOG.info("export GTFS from copied namespace");
            File tempFile = exportGtfs(copyResult.uniqueIdentifier, dataSource, true);
            assertThatExportedGtfsMeetsExpectations(tempFile, persistenceExpectations, true);
        } catch (IOException | SQLException e) {
            e.printStackTrace();
            TestUtils.dropDB(testDBName);
            return false;
        } catch (AssertionError e) {
            TestUtils.dropDB(testDBName);
            throw e;
        }
        return true;
    }

    /**
     * Run through the list of persistence expectations to make sure that the feed was imported properly into the
     * database.
     */
    private void assertThatImportedGtfsMeetsExpectations(
        Connection connection,
        String namespace,
        PersistenceExpectation[] persistenceExpectations,
        ErrorExpectation[] errorExpectations,
        boolean isEditorDatabase
    ) throws SQLException {
        // Store field mismatches here (to provide assertion statements with more details).
        Multimap<String, ValuePair> fieldsWithMismatches = ArrayListMultimap.create();
        // Check that no validators failed during validation in non-editor databases only (validators do not run
        // when creating an editor database).
        if (!isEditorDatabase) {
            assertThat(
                "One or more validators failed during GTFS import.",
                countValidationErrorsOfType(connection, namespace, NewGTFSErrorType.VALIDATOR_FAILED),
                equalTo(0)
            );
        }
        // run through testing expectations
        LOG.info("testing expectations of record storage in the database");
        for (PersistenceExpectation persistenceExpectation : persistenceExpectations) {
            if (persistenceExpectation.appliesToEditorDatabaseOnly && !isEditorDatabase) continue;
            // select all entries from a table
            String sql = String.format(
                "select * from %s.%s",
                namespace,
                persistenceExpectation.tableName
            );
            LOG.info(sql);
            ResultSet rs = connection.prepareStatement(sql).executeQuery();
            boolean foundRecord = false;
            int numRecordsSearched = 0;
            while (rs.next()) {
                numRecordsSearched++;
                LOG.info("record {} in ResultSet", numRecordsSearched);
                boolean allFieldsMatch = true;
                for (RecordExpectation recordExpectation: persistenceExpectation.recordExpectations) {
                    switch (recordExpectation.expectedFieldType) {
                        case DOUBLE:
                            double doubleVal = rs.getDouble(recordExpectation.fieldName);
                            LOG.info("{}: {}", recordExpectation.fieldName, doubleVal);
                            if (doubleVal != recordExpectation.doubleExpectation) {
                                allFieldsMatch = false;
                            }
                            break;
                        case INT:
                            int intVal = rs.getInt(recordExpectation.fieldName);
                            LOG.info("{}: {}", recordExpectation.fieldName, intVal);
                            if (intVal != recordExpectation.intExpectation) {
                                fieldsWithMismatches.put(
                                        recordExpectation.fieldName,
                                        new ValuePair(recordExpectation.stringExpectation, intVal)
                                );
                                allFieldsMatch = false;
                            }
                            break;
                        case STRING:
                            String strVal = rs.getString(recordExpectation.fieldName);
                            LOG.info("{}: {}", recordExpectation.fieldName, strVal);
                            if (strVal == null && recordExpectation.stringExpectation == null) {
                                break;
                            } else if (strVal == null || !strVal.equals(recordExpectation.stringExpectation)) {
                                fieldsWithMismatches.put(
                                    recordExpectation.fieldName,
                                    new ValuePair(recordExpectation.stringExpectation, strVal)
                                );
                                LOG.error("Expected {}, found {}", recordExpectation.stringExpectation, strVal);
                                allFieldsMatch = false;
                            }
                            break;

                    }
                    if (!allFieldsMatch) {
                        break;
                    }
                }
                // all fields match expectations!  We have found the record.
                if (allFieldsMatch) {
                    LOG.info("Database record satisfies expectations.");
                    foundRecord = true;
                    break;
                } else {
                    LOG.error("Persistence mismatch on record {}", numRecordsSearched);
                }
            }
            assertThatDatabasePersistenceExpectationRecordWasFound(
                persistenceExpectation,
                numRecordsSearched,
                foundRecord,
                fieldsWithMismatches
            );
        }
        // Skip error expectation analysis on editor database
        if (isEditorDatabase) {
            LOG.info("Skipping error expectations for non-editor database.");
            return;
        }
        // Expect zero errors if errorExpectations is null.
        if (errorExpectations == null) errorExpectations = new ErrorExpectation[]{};
        // Check that error expectations match errors stored in database.
        LOG.info("Checking {} error expectations", errorExpectations.length);
        // select all entries from error table
        String sql = String.format("select * from %s.errors", namespace);
        LOG.info(sql);
        ResultSet rs = connection.prepareStatement(sql).executeQuery();
        int errorCount = 0;
        Iterator<ErrorExpectation> errorExpectationIterator = Arrays.stream(errorExpectations).iterator();
        while (rs.next()) {
            errorCount++;
            String errorType = rs.getString("error_type");
            String entityType = rs.getString("entity_type");
            String entityId = rs.getString("entity_id");
            String badValue = rs.getString("bad_value");
            LOG.info("Found error {}: {} {} {} {}", errorCount, errorType, entityId, entityType, badValue);
            // Skip error expectation if not exists. But continue iteration to count all errors.
            if (!errorExpectationIterator.hasNext()) continue;
            ErrorExpectation errorExpectation = errorExpectationIterator.next();
            LOG.info("Expecting error {}: {}", errorCount, errorExpectation.errorTypeMatcher);
            // Error expectation must contain error type matcher. The others are optional.
            assertThat(errorType, errorExpectation.errorTypeMatcher);
            if (errorExpectation.entityTypeMatcher != null) assertThat(entityType, errorExpectation.entityTypeMatcher);
            if (errorExpectation.entityIdMatcher != null) assertThat(entityId, errorExpectation.entityIdMatcher);
            if (errorExpectation.badValueMatcher != null) assertThat(badValue, errorExpectation.badValueMatcher);
        }
        assertThat(
            "Error count is equal to number of error expectations.",
            errorCount,
            equalTo(errorExpectations.length));
    }

    private static int countValidationErrorsOfType(
            Connection connection,
            String namespace,
            NewGTFSErrorType errorType
    ) throws SQLException {
        String errorCheckSql = String.format(
                "select * from %s.errors where error_type = '%s'",
                namespace,
                errorType);
        LOG.info(errorCheckSql);
        ResultSet errorResults = connection.prepareStatement(errorCheckSql).executeQuery();
        int errorCount = 0;
        while (errorResults.next()) {
            errorCount++;
        }
        return errorCount;
    }

    /**
     * Helper to assert that the GTFS that was exported to a zip file matches all data expectations defined in the
     * persistence expectations.
     */
    private void assertThatExportedGtfsMeetsExpectations(
        File tempFile,
        PersistenceExpectation[] persistenceExpectations,
        boolean fromEditor
    ) throws IOException {
        LOG.info("testing expectations of csv outputs in an exported gtfs");

        ZipFile gtfsZipfile = new ZipFile(tempFile.getAbsolutePath());

        // iterate through all expectations
        for (PersistenceExpectation persistenceExpectation : persistenceExpectations) {
            if (persistenceExpectation.appliesToEditorDatabaseOnly) continue;
            // No need to check that errors were exported because it is an internal table only.
            if ("errors".equals(persistenceExpectation.tableName)) continue;
            final String tableFileName = persistenceExpectation.tableName + ".txt";
            LOG.info(String.format("reading table: %s", tableFileName));

            ZipEntry entry = gtfsZipfile.getEntry(tableFileName);

            // ensure file exists in zip
            if (entry == null) {
                throw new AssertionError(
                    String.format("expected table %s not found in outputted zip file", tableFileName)
                );
            }

            // prepare to read the file
            InputStream zipInputStream = gtfsZipfile.getInputStream(entry);
            // Skip any byte order mark that may be present. Files must be UTF-8,
            // but the GTFS spec says that "files that include the UTF byte order mark are acceptable".
            InputStream bomInputStream = new BOMInputStream(zipInputStream);
            CsvReader csvReader = new CsvReader(bomInputStream, ',', Charset.forName("UTF8"));
            csvReader.readHeaders();

            boolean foundRecord = false;
            int numRecordsSearched = 0;

            // read each record
            while (csvReader.readRecord() && !foundRecord) {
                numRecordsSearched++;
                LOG.info(String.format("record %d in csv file", numRecordsSearched));
                boolean allFieldsMatch = true;

                // iterate through all rows in record to determine if it's the one we're looking for
                for (RecordExpectation recordExpectation: persistenceExpectation.recordExpectations) {
                    String val = csvReader.get(recordExpectation.fieldName);
                    String expectation = recordExpectation.getStringifiedExpectation(fromEditor);
                    LOG.info(String.format(
                        "%s: %s (Expectation: %s)",
                        recordExpectation.fieldName,
                        val,
                        expectation
                    ));
                    if (val.isEmpty() && expectation == null) {
                        // First check that the csv value is an empty string and that the expectation is null. Null
                        // exported from the database to a csv should round trip into an empty string, so this meets the
                        // expectation.
                        break;
                    } else if (!val.equals(expectation)) {
                        // sometimes there are slight differences in decimal precision in various fields
                        // check if the decimal delta is acceptable
                        if (equalsWithNumericDelta(val, recordExpectation)) continue;
                        allFieldsMatch = false;
                        break;
                    }
                }
                // all fields match expectations!  We have found the record.
                if (allFieldsMatch) {
                    LOG.info("CSV record satisfies expectations.");
                    foundRecord = true;
                }
            }
            assertThatCSVPersistenceExpectationRecordWasFound(
                persistenceExpectation,
                tableFileName,
                numRecordsSearched,
                foundRecord
            );
        }
    }

    /**
     * Check whether a potentially numeric value is equal given potentially small decimal deltas
     */
    private boolean equalsWithNumericDelta(String val, RecordExpectation recordExpectation) {
        if (recordExpectation.expectedFieldType != ExpectedFieldType.DOUBLE) return false;
        double d;
        try {
            d = Double.parseDouble(val);
        }
        catch(NumberFormatException nfe)
        {
            return false;
        }
        return Math.abs(d - recordExpectation.doubleExpectation) < recordExpectation.acceptedDelta;
    }

    /**
     * Helper that calls assertion with corresponding context for better error reporting.
     */
    private void assertThatDatabasePersistenceExpectationRecordWasFound(
        PersistenceExpectation persistenceExpectation,
        int numRecordsSearched,
        boolean foundRecord,
        Multimap<String, ValuePair> fieldsWithMismatches
    ) {
        assertThatPersistenceExpectationRecordWasFound(
            persistenceExpectation,
            String.format("Database table `%s`", persistenceExpectation.tableName),
            numRecordsSearched,
            foundRecord,
            fieldsWithMismatches
        );
    }

    /**
     * Helper that calls assertion with corresponding context for better error reporting.
     */
    private void assertThatCSVPersistenceExpectationRecordWasFound(
        PersistenceExpectation persistenceExpectation,
        String tableFileName,
        int numRecordsSearched,
        boolean foundRecord
    ) {
        assertThatPersistenceExpectationRecordWasFound(
            persistenceExpectation,
            String.format("CSV file `%s`", tableFileName),
            numRecordsSearched,
            foundRecord,
            null
        );
    }

    /**
     * Helper method to make sure a persistence expectation was actually found after searching through records
     */
    private void assertThatPersistenceExpectationRecordWasFound(
        PersistenceExpectation persistenceExpectation,
        String contextDescription,
        int numRecordsSearched,
        boolean foundRecord,
        Multimap<String, ValuePair> mismatches
    ) {
        contextDescription = String.format("in the %s", contextDescription);
        // Assert that more than 0 records were found
        assertThat(
            String.format("No records found %s", contextDescription),
            numRecordsSearched,
            ComparatorMatcherBuilder.<Integer>usingNaturalOrdering().greaterThan(0)
        );
        // If the record wasn't found, but at least one mismatching record was found, return info about the record that
        // was found to attempt to aid with debugging.
        if (!foundRecord && mismatches != null) {
            for (String field : mismatches.keySet()) {
                Collection<ValuePair> valuePairs = mismatches.get(field);
                for (ValuePair valuePair : valuePairs) {
                    assertThat(
                        String.format(
                            "The value expected for %s was not found %s. NOTE: there could be other values, but the first found value is shown.",
                            field,
                            contextDescription
                        ),
                        valuePair.found,
                        equalTo(valuePair.expected)
                    );
                }
            }
        } else {
            // Assert that the record was found
            assertThat(
                String.format(
                    "The record as defined in the PersistenceExpectation was not found %s. Unfound Record: %s",
                    contextDescription,
                    persistenceExpectation.toString()
                ),
                foundRecord,
                equalTo(true)
            );
        }
    }

    /**
     * Persistence expectations for use with the GTFS contained within the "fake-agency" resources folder.
     */
    private PersistenceExpectation[] fakeAgencyPersistenceExpectations = new PersistenceExpectation[]{
        new PersistenceExpectation(
            "agency",
            new RecordExpectation[]{
                new RecordExpectation("agency_id", "1"),
                new RecordExpectation("agency_name", "Fake Transit"),
                new RecordExpectation("agency_timezone", "America/Los_Angeles")
            }
        ),
        new PersistenceExpectation(
            "calendar",
            new RecordExpectation[]{
                new RecordExpectation(
                    "service_id", "04100312-8fe1-46a5-a9f2-556f39478f57"
                ),
                new RecordExpectation("monday", 1),
                new RecordExpectation("tuesday", 1),
                new RecordExpectation("wednesday", 1),
                new RecordExpectation("thursday", 1),
                new RecordExpectation("friday", 1),
                new RecordExpectation("saturday", 1),
                new RecordExpectation("sunday", 1),
                new RecordExpectation("start_date", "20170915"),
                new RecordExpectation("end_date", "20170917")
            }
        ),
        new PersistenceExpectation(
            "calendar_dates",
            new RecordExpectation[]{
                new RecordExpectation(
                    "service_id", "04100312-8fe1-46a5-a9f2-556f39478f57"
                ),
                new RecordExpectation("date", 20170916),
                new RecordExpectation("exception_type", 2)
            }
        ),
        new PersistenceExpectation(
            "fare_attributes",
            new RecordExpectation[]{
                new RecordExpectation("fare_id", "route_based_fare"),
                new RecordExpectation("price", 1.23, 0),
                new RecordExpectation("currency_type", "USD")
            }
        ),
        new PersistenceExpectation(
            "fare_rules",
            new RecordExpectation[]{
                new RecordExpectation("fare_id", "route_based_fare"),
                new RecordExpectation("route_id", "1")
            }
        ),
        new PersistenceExpectation(
            "feed_info",
            new RecordExpectation[]{
                new RecordExpectation("feed_id", "fake_transit"),
                new RecordExpectation("feed_publisher_name", "Conveyal"),
                new RecordExpectation(
                    "feed_publisher_url", "http://www.conveyal.com"
                ),
                new RecordExpectation("feed_lang", "en"),
                new RecordExpectation("feed_version", "1.0")
            }
        ),
        new PersistenceExpectation(
            "frequencies",
            new RecordExpectation[]{
                new RecordExpectation("trip_id", "frequency-trip"),
                new RecordExpectation("start_time", 28800, "08:00:00"),
                new RecordExpectation("end_time", 32400, "09:00:00"),
                new RecordExpectation("headway_secs", 1800),
                new RecordExpectation("exact_times", 0)
            }
        ),
        new PersistenceExpectation(
            "routes",
            new RecordExpectation[]{
                new RecordExpectation("agency_id", "1"),
                new RecordExpectation("route_id", "1"),
                new RecordExpectation("route_short_name", "1"),
                new RecordExpectation("route_long_name", "Route 1"),
                new RecordExpectation("route_type", 3),
                new RecordExpectation("route_color", "7CE6E7")
            }
        ),
        new PersistenceExpectation(
            "shapes",
            new RecordExpectation[]{
                new RecordExpectation(
                    "shape_id", "5820f377-f947-4728-ac29-ac0102cbc34e"
                ),
                new RecordExpectation("shape_pt_lat", 37.061172, 0.00001),
                new RecordExpectation("shape_pt_lon", -122.007500, 0.00001),
                new RecordExpectation("shape_pt_sequence", 2),
                new RecordExpectation("shape_dist_traveled", 7.4997067, 0.01)
            }
        ),
        new PersistenceExpectation(
            "stop_times",
            new RecordExpectation[]{
                new RecordExpectation(
                    "trip_id", "a30277f8-e50a-4a85-9141-b1e0da9d429d"
                ),
                new RecordExpectation("arrival_time", 25200, "07:00:00"),
                new RecordExpectation("departure_time", 25200, "07:00:00"),
                new RecordExpectation("stop_id", "4u6g"),
                new RecordExpectation("stop_sequence", 1),
                new RecordExpectation("pickup_type", 0),
                new RecordExpectation("stop_headsign", "Test stop headsign"),
                new RecordExpectation("drop_off_type", 0),
                new RecordExpectation("shape_dist_traveled", 0.0, 0.01)
            }
        ),
        new PersistenceExpectation(
            "trips",
            new RecordExpectation[]{
                new RecordExpectation(
                    "trip_id", "a30277f8-e50a-4a85-9141-b1e0da9d429d"
                ),
                new RecordExpectation(
                    "service_id", "04100312-8fe1-46a5-a9f2-556f39478f57"
                ),
                new RecordExpectation("route_id", "1"),
                new RecordExpectation("direction_id", 0),
                new RecordExpectation(
                    "shape_id", "5820f377-f947-4728-ac29-ac0102cbc34e"
                ),
                new RecordExpectation("bikes_allowed", 0),
                new RecordExpectation("wheelchair_accessible", 0)
            }
        ),
        new PersistenceExpectation(
            "transfers",
            new RecordExpectation[]{
                new RecordExpectation("from_stop_id", "4u6g"),
                new RecordExpectation("to_stop_id", "johv"),
                new RecordExpectation("from_trip_id", "a30277f8-e50a-4a85-9141-b1e0da9d429d"),
                new RecordExpectation("to_trip_id", "frequency-trip"),
                new RecordExpectation("from_route_id", "1"),
                new RecordExpectation("to_route_id", "1"),
                new RecordExpectation("transfer_type", "1"),
                new RecordExpectation("min_transfer_time", "60")
            }
        )
    };

    /**
     * Update persistence expectations to expect normalized stop_sequence values (zero-based, incrementing).
     */
    private PersistenceExpectation[] updatePersistenceExpectationsWithNormalizedStopTimesSequence(
        PersistenceExpectation[] inputExpectations
    ) {
        PersistenceExpectation[] persistenceExpectations = new PersistenceExpectation[inputExpectations.length];
        // Add all of the table expectations.
        for (int i = 0; i < inputExpectations.length; i++) {
            // Collect record expectations.
            PersistenceExpectation inputExpectation = inputExpectations[i];
            RecordExpectation[] newRecordExpectations = new RecordExpectation[inputExpectation.recordExpectations.length];
            for (int j = 0; j < inputExpectation.recordExpectations.length; j++) {
                RecordExpectation newRecordExpectation = inputExpectation.recordExpectations[j].clone();
                // Update the stop_sequence expectation to be normalized.
                if (newRecordExpectation.fieldName.equals("stop_sequence")) {
                    newRecordExpectation.intExpectation = 0;
                }
                newRecordExpectations[j] = newRecordExpectation;
            }
            // Once cloning/updating has been done for all record expectations, add the new table expectation to the
            // array.
            persistenceExpectations[i] = new PersistenceExpectation(
                inputExpectation.tableName,
                newRecordExpectations,
                inputExpectation.appliesToEditorDatabaseOnly
            );
        }
        return persistenceExpectations;
    }
}
