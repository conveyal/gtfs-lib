package com.conveyal.gtfs;


import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.loader.FeedLoadResult;
import com.conveyal.gtfs.loader.SnapshotResult;
import com.conveyal.gtfs.storage.ErrorExpectation;
import com.conveyal.gtfs.storage.ExpectedFieldType;
import com.conveyal.gtfs.storage.PersistenceExpectation;
import com.conveyal.gtfs.storage.RecordExpectation;
import com.conveyal.gtfs.util.InvalidNamespaceException;
import com.conveyal.gtfs.validator.ValidatorCreator;
import com.conveyal.gtfs.validator.FeedValidator;
import com.conveyal.gtfs.validator.MTCValidator;
import com.conveyal.gtfs.validator.ValidationResult;
import com.csvreader.CsvReader;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.hamcrest.Matcher;
import org.hamcrest.comparator.ComparatorMatcherBuilder;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

/**
 * A test suite for the {@link GTFS} Class.
 */
public class GTFSTest {
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private static final String JDBC_URL = "jdbc:postgresql://localhost";
    private static final Logger LOG = LoggerFactory.getLogger(GTFSTest.class);

    private List<Class<? extends FeedValidator>> extraValidatorClasses = new ArrayList<>();

    // setup a stream to capture the output from the program
    @Before
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
    public void canLoadAndExportSimpleAgencyInSubDirectory() {
        String zipFileName = null;
        // Get filename for fake-agency resource
        String resourceFolder = TestUtils.getResourceFileName("fake-agency");
        // Recursively copy folder into temp directory, which we zip up and run the integration test on.
        File tempDir = Files.createTempDir();
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
                    // the string expectation for stop_sequence is different because of how stop_times are
                    // converted to 0-based indexes in Table.normalizeAndCloneStopTimes
                    new RecordExpectation("stop_sequence", 1, "1", "0"),
                    new RecordExpectation("pickup_type", 0),
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
     * Tests that a GTFS feed with long field values generates corresponding
     * validation errors per MTC guidelines.
     */
    @Test
    public void canLoadFeedWithLongFieldValues () {
        extraValidatorClasses.add(MTCValidator.class);

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
     * Shorthand for next method.
     */
    private boolean runIntegrationTestOnFolder(
        String folderName,
        Matcher<Object> fatalExceptionExpectation,
        PersistenceExpectation[] persistenceExpectations,
        ErrorExpectation[] errorExpectations
    ) {
        return runIntegrationTestOnFolder(
            folderName,
            fatalExceptionExpectation,
            persistenceExpectations,
            errorExpectations,
            null
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
        ValidatorCreator customValidator
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
            customValidator
        );
    }

    /**
     * Shorthand for next method.
     */
    private boolean runIntegrationTestOnZipFile(
        String zipFileName,
        Matcher<Object> fatalExceptionExpectation,
        PersistenceExpectation[] persistenceExpectations,
        ErrorExpectation[] errorExpectations
    ) {
        return runIntegrationTestOnZipFile(
            zipFileName,
            fatalExceptionExpectation,
            persistenceExpectations,
            errorExpectations,
            null
        );
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
        ValidatorCreator customValidator
    ) {
        String testDBName = TestUtils.generateNewDB();
        String dbConnectionUrl = String.join("/", JDBC_URL, testDBName);
        DataSource dataSource = GTFS.createDataSource(
            dbConnectionUrl,
            null,
            null
        );

        String namespace;

        // Verify that loading the feed completes and data is stored properly
        try (Connection connection = dataSource.getConnection()) {
            // load and validate feed
            LOG.info("load and validate GTFS file {}", zipFileName);
            FeedLoadResult loadResult = GTFS.load(zipFileName, dataSource);
            ValidationResult validationResult = GTFS.validate(loadResult.uniqueIdentifier, dataSource, customValidator);

            assertThat(validationResult.fatalException, is(fatalExceptionExpectation));
            namespace = loadResult.uniqueIdentifier;
            assertThatImportedGtfsMeetsExpectations(connection, namespace, persistenceExpectations, errorExpectations);
        } catch (SQLException e) {
            TestUtils.dropDB(testDBName);
            e.printStackTrace();
            return false;
        } catch (AssertionError e) {
            TestUtils.dropDB(testDBName);
            throw e;
        }

        // Verify that exporting the feed (in non-editor mode) completes and data is outputted properly
        try {
            LOG.info("export GTFS from created namespace");
            File tempFile = exportGtfs(namespace, dataSource, false);
            assertThatExportedGtfsMeetsExpectations(tempFile, persistenceExpectations, false);
        } catch (IOException e) {
            TestUtils.dropDB(testDBName);
            e.printStackTrace();
            return false;
        } catch (AssertionError e) {
            TestUtils.dropDB(testDBName);
            throw e;
        }

        // Verify that making a snapshot from an existing feed database, then exporting that snapshot to a GTFS zip file
        // works as expected
        try {
            LOG.info("copy GTFS from created namespace");
            SnapshotResult copyResult = GTFS.makeSnapshot(namespace, dataSource);
            assertThatSnapshotIsErrorFree(copyResult);
            LOG.info("export GTFS from copied namespace");
            File tempFile = exportGtfs(copyResult.uniqueIdentifier, dataSource, true);
            assertThatExportedGtfsMeetsExpectations(tempFile, persistenceExpectations, true);
        } catch (IOException e) {
            e.printStackTrace();
            TestUtils.dropDB(testDBName);
            return false;
        } catch (AssertionError e) {
            TestUtils.dropDB(testDBName);
            throw e;
        }

        // Verify that deleting a feed works as expected.
        try (Connection connection = dataSource.getConnection()) {
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
            e.printStackTrace();
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
     * Run through the list of persistence expectations to make sure that the feed was imported properly into the
     * database.
     */
    private void assertThatImportedGtfsMeetsExpectations(
        Connection connection,
        String namespace,
        PersistenceExpectation[] persistenceExpectations,
        ErrorExpectation[] errorExpectations
    ) throws SQLException {
        // Store field mismatches here (to provide assertion statements with more details).
        Multimap<String, ValuePair> fieldsWithMismatches = ArrayListMultimap.create();
        // Check that no validators failed during validation.
        assertThat(
            "One or more validators failed during GTFS import.",
            countValidationErrorsOfType(connection, namespace, NewGTFSErrorType.VALIDATOR_FAILED),
            equalTo(0)
        );
        // run through testing expectations
        LOG.info("testing expectations of record storage in the database");
        for (PersistenceExpectation persistenceExpectation : persistenceExpectations) {
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
            assertThatPersistenceExpectationRecordWasFound(numRecordsSearched, foundRecord, fieldsWithMismatches);
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
            assertThatPersistenceExpectationRecordWasFound(numRecordsSearched, foundRecord, null);
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
     * Helper method to make sure a persistence expectation was actually found after searching through records
     */
    private void assertThatPersistenceExpectationRecordWasFound(
        int numRecordsSearched,
        boolean foundRecord,
        Multimap<String, ValuePair> mismatches
    ) {
        assertThat(
            "No records found in the ResultSet/CSV file",
            numRecordsSearched,
            ComparatorMatcherBuilder.<Integer>usingNaturalOrdering().greaterThan(0)
        );
        if (mismatches != null) {
            for (String field : mismatches.keySet()) {
                Collection<ValuePair> valuePairs = mismatches.get(field);
                for (ValuePair valuePair : valuePairs) {
                    assertThat(
                        String.format("The value expected for %s was not found", field),
                        valuePair.found,
                        equalTo(valuePair.expected)
                    );
                }
            }
        } else {
            assertThat(
                "The record as defined in the PersistenceExpectation was not found.",
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
                    // the string expectation for stop_sequence is different because of how stop_times are
                    // converted to 0-based indexes in Table.normalizeAndCloneStopTimes
                    new RecordExpectation("stop_sequence", 1, "1", "0"),
                    new RecordExpectation("pickup_type", 0),
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
            )
    };
}
