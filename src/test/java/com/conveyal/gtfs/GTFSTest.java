package com.conveyal.gtfs;


import com.conveyal.gtfs.loader.FeedLoadResult;
import com.conveyal.gtfs.validator.ValidationResult;
import com.csvreader.CsvReader;
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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
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
    private static final Logger LOG = LoggerFactory.getLogger(GTFSTest.class);

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
        assertThat(
            runIntegrationTestOnFolder(
                "fake-agency",
                nullValue(),
                fakeAgencyPersistenceExpectations
            ),
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
        // TODO Add error expectations argument that expects NewGTFSErrorType.TABLE_IN_SUBDIRECTORY error type.
        assertThat(
            runIntegrationTestOnZipFile(
                zipFileName,
                nullValue(),
                fakeAgencyPersistenceExpectations
            ),
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
        assertThat(
            runIntegrationTestOnFolder(
                "fake-agency-only-calendar-dates",
                nullValue(),
                persistenceExpectations
            ),
            equalTo(true)
        );
    }


    /**
     * A helper method that will zip a specified folder in test/main/resources and call
     * {@link #runIntegrationTestOnZipFile(String, Matcher, PersistenceExpectation[])} on that file.
     */
    private boolean runIntegrationTestOnFolder(
        String folderName,
        Matcher<Object> fatalExceptionExpectation,
        PersistenceExpectation[] persistenceExpectations
    ) {
        // zip up test folder into temp zip file
        String zipFileName = null;
        try {
            zipFileName = TestUtils.zipFolderFiles(folderName, true);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return runIntegrationTestOnZipFile(zipFileName, fatalExceptionExpectation, persistenceExpectations);
    }

    /**
     * A helper method that will run GTFS.main with a certain zip file.
     * This tests whether a GTFS zip file can be loaded without any errors.
     *
     * After the GTFS is loaded, this will also initiate an export of a GTFS from the database and check
     * the integrity of the exported GTFS.
     */
    private boolean runIntegrationTestOnZipFile(
        String zipFileName,
        Matcher<Object> fatalExceptionExpectation,
        PersistenceExpectation[] persistenceExpectations
    ) {
        String newDBName = TestUtils.generateNewDB();
        String dbConnectionUrl = String.format("jdbc:postgresql://localhost/%s", newDBName);
        DataSource dataSource = GTFS.createDataSource(
            dbConnectionUrl,
            null,
            null
        );

        String namespace;

        // Verify that loading the feed completes and data is stored properly
        try {
            // load and validate feed
            LOG.info("load and validate feed");
            FeedLoadResult loadResult = GTFS.load(zipFileName, dataSource);
            ValidationResult validationResult = GTFS.validate(loadResult.uniqueIdentifier, dataSource);

            assertThat(validationResult.fatalException, is(fatalExceptionExpectation));
            namespace = loadResult.uniqueIdentifier;

            assertThatImportedGtfsMeetsExpectations(dbConnectionUrl, namespace, persistenceExpectations);
        } catch (SQLException e) {
            TestUtils.dropDB(newDBName);
            e.printStackTrace();
            return false;
        } catch (AssertionError e) {
            TestUtils.dropDB(newDBName);
            throw e;
        }

        // Verify that exporting the feed (in non-editor mode) completes and data is outputed properly
        try {
            LOG.info("export GTFS from created namespase");
            File tempFile = exportGtfs(namespace, dataSource, false);
            assertThatExportedGtfsMeetsExpectations(tempFile, persistenceExpectations, false);
        } catch (IOException e) {
            TestUtils.dropDB(newDBName);
            e.printStackTrace();
            return false;
        } catch (AssertionError e) {
            TestUtils.dropDB(newDBName);
            throw e;
        }

        // Verify that making a snapshot from an existing feed database, then exporting that snapshot to a GTFS zip file
        // works as expected
        try {
            LOG.info("copy GTFS from created namespase");
            FeedLoadResult copyResult = GTFS.makeSnapshot(namespace, dataSource);
            LOG.info("export GTFS from copied namespase");
            File tempFile = exportGtfs(copyResult.uniqueIdentifier, dataSource, true);
            assertThatExportedGtfsMeetsExpectations(tempFile, persistenceExpectations, true);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {
            TestUtils.dropDB(newDBName);
        }

        return true;
    }

    /**
     * Helper function to export a GTFS from the database to a temporary zip file.
     */
    private File exportGtfs(String namespace, DataSource dataSource, boolean fromEditor) throws IOException {
        File tempFile = File.createTempFile("snapshot", ".zip");
        GTFS.export(namespace, tempFile.getAbsolutePath(), dataSource, fromEditor);
        return tempFile;
    }

    /**
     * Run through the list of persistence expectations to make sure that the feed was imported properly into the
     * database.
     */
    private void assertThatImportedGtfsMeetsExpectations(
        String dbConnectionUrl,
        String namespace,
        PersistenceExpectation[] persistenceExpectations
    ) throws SQLException {
        // run through testing expectations
        LOG.info("testing expecations of record storage in the database");
        Connection conn = DriverManager.getConnection(dbConnectionUrl);
        for (PersistenceExpectation persistenceExpectation : persistenceExpectations) {
            // select all entries from a table
            String sql = String.format(
                "select * from %s.%s",
                namespace,
                persistenceExpectation.tableName
            );
            LOG.info(sql);
            ResultSet rs = conn.prepareStatement(sql).executeQuery();
            boolean foundRecord = false;
            int numRecordsSearched = 0;
            while (rs.next()) {
                numRecordsSearched++;
                LOG.info(String.format("record %d in ResultSet", numRecordsSearched));
                boolean allFieldsMatch = true;
                for (RecordExpectation recordExpectation: persistenceExpectation.recordExpectations) {
                    switch (recordExpectation.expectedFieldType) {
                        case DOUBLE:
                            LOG.info(String.format(
                                "%s: %f",
                                recordExpectation.fieldName,
                                rs.getDouble(recordExpectation.fieldName)
                            ));
                            if (rs.getDouble(recordExpectation.fieldName) != recordExpectation.doubleExpectation) {
                                allFieldsMatch = false;
                            }
                            break;
                        case INT:
                            LOG.info(String.format(
                                "%s: %d",
                                recordExpectation.fieldName,
                                rs.getInt(recordExpectation.fieldName)
                            ));
                            if (rs.getInt(recordExpectation.fieldName) != recordExpectation.intExpectation) {
                                allFieldsMatch = false;
                            }
                            break;
                        case STRING:
                            LOG.info(String.format(
                                "%s: %s",
                                recordExpectation.fieldName,
                                rs.getString(recordExpectation.fieldName)
                            ));
                            if (!rs.getString(recordExpectation.fieldName).equals(recordExpectation.stringExpectation)) {
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
                }
            }
            assertThatPersistenceExpectationRecordWasFound(numRecordsSearched, foundRecord);
        }
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
        LOG.info("testing expecations of csv outputs in an exported gtfs");

        ZipFile gtfsZipfile = new ZipFile(tempFile.getAbsolutePath());

        // iterate through all expectations
        for (PersistenceExpectation persistenceExpectation : persistenceExpectations) {
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
                    if (!val.equals(expectation)) {
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
            assertThatPersistenceExpectationRecordWasFound(numRecordsSearched, foundRecord);
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
    private void assertThatPersistenceExpectationRecordWasFound(int numRecordsSearched, boolean foundRecord) {
        assertThat(
            "No records found in the ResultSet/CSV file",
            numRecordsSearched,
            ComparatorMatcherBuilder.<Integer>usingNaturalOrdering().greaterThan(0)
        );
        assertThat(
            "The record as defined in the PersistenceExpectation was not found.",
            foundRecord,
            equalTo(true)
        );
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
                    new RecordExpectation("feed_publisher_name", "Conveyal"
                    ),
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

    /**
     * A helper class to verify that data got stored in a particular table.
     */
    private class PersistenceExpectation {
        public String tableName;
        // each persistence expectation has an array of record expectations which all must reference a single row
        // if looking for multiple records in the same table,
        // create numerous PersistenceExpectations with the same tableName
        public RecordExpectation[] recordExpectations;


        public PersistenceExpectation(String tableName, RecordExpectation[] recordExpectations) {
            this.tableName = tableName;
            this.recordExpectations = recordExpectations;
        }
    }

    private enum ExpectedFieldType {
        INT,
        DOUBLE, STRING
    }

    /**
     * A helper class to verify that data got stored in a particular record.
     */
    private class RecordExpectation {
        public double acceptedDelta;
        public double doubleExpectation;
        public String editorExpectation;
        public ExpectedFieldType expectedFieldType;
        public String fieldName;
        public int intExpectation;
        public String stringExpectation;
        public boolean stringExpectationInCSV = false;
        public boolean editorStringExpectation = false;

        public RecordExpectation(String fieldName, int intExpectation) {
            this.fieldName = fieldName;
            this.expectedFieldType = ExpectedFieldType.INT;
            this.intExpectation = intExpectation;
        }

        /**
         * This extra constructor is a bit hacky in that it is only used for certain records that have
         * an int type when stored in the database, but a string type when exported to GTFS
         */
        public RecordExpectation(String fieldName, int intExpectation, String stringExpectation) {
            this.fieldName = fieldName;
            this.expectedFieldType = ExpectedFieldType.INT;
            this.intExpectation = intExpectation;
            this.stringExpectation = stringExpectation;
            this.stringExpectationInCSV = true;
        }

        /**
         * This extra constructor is a also hacky in that it is only used for records that have
         * an int type when stored in the database, and different values in the CSV export depending on
         * whether or not it is a snapshot from the editor.  Currently this only applies to stop_times.stop_sequence
         */
        public RecordExpectation(String fieldName, int intExpectation, String stringExpectation, String editorExpectation) {
            this.fieldName = fieldName;
            this.expectedFieldType = ExpectedFieldType.INT;
            this.intExpectation = intExpectation;
            this.stringExpectation = stringExpectation;
            this.stringExpectationInCSV = true;
            this.editorStringExpectation = true;
            this.editorExpectation = editorExpectation;
        }

        public RecordExpectation(String fieldName, String stringExpectation) {
            this.fieldName = fieldName;
            this.expectedFieldType = ExpectedFieldType.STRING;
            this.stringExpectation = stringExpectation;
        }

        public RecordExpectation(String fieldName, double doubleExpectation, double acceptedDelta) {
            this.fieldName = fieldName;
            this.expectedFieldType = ExpectedFieldType.DOUBLE;
            this.doubleExpectation = doubleExpectation;
            this.acceptedDelta = acceptedDelta;
        }

        public String getStringifiedExpectation(boolean fromEditor) {
            if (fromEditor && editorStringExpectation) return editorExpectation;
            if (stringExpectationInCSV) return stringExpectation;
            switch (expectedFieldType) {
                case DOUBLE:
                    return String.valueOf(doubleExpectation);
                case INT:
                    return String.valueOf(intExpectation);
                case STRING:
                    return stringExpectation;
                default:
                    return null;
            }
        }
    }
}
