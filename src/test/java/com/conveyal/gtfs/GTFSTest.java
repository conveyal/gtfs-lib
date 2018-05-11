package com.conveyal.gtfs;


import com.conveyal.gtfs.loader.FeedLoadResult;
import com.conveyal.gtfs.validator.ValidationResult;
import org.hamcrest.Matcher;
import org.hamcrest.comparator.ComparatorMatcherBuilder;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.conveyal.gtfs.GTFS.createDataSource;
import static com.conveyal.gtfs.GTFS.load;
import static com.conveyal.gtfs.GTFS.validate;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

/**
 * A test suite for the GTFS Class
 */
public class GTFSTest {
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private static final Logger LOG = LoggerFactory.getLogger(GTFSTest.class);

    // a helper class to verify that data got stored in a particular table
    private class PersistanceExpectation {
        public String tableName;
        // each persistance expectation has an array of record expectations which all must reference a single row
        // if looking for multiple records in the same table,
        // create numerous PersistanceExpectations with the same tableName
        public RecordExpectation[] recordExpectations;


        public PersistanceExpectation(String tableName, RecordExpectation[] recordExpectations) {
            this.tableName = tableName;
            this.recordExpectations = recordExpectations;
        }
    }

    private enum ExpectedFieldType {
        INT,
        DOUBLE, STRING
    }

    // a helper class to verify that data got stored in a particular record
    private class RecordExpectation {
        public String fieldName;
        public ExpectedFieldType expectedFieldType;
        // have a bunch of different types of expectations because java is strongly typed
        public double doubleExpectation;
        public int intExpectation;
        public String stringExpectation;

        public RecordExpectation(String fieldName, ExpectedFieldType expectedFieldType, int intExpectation) {
            this.fieldName = fieldName;
            this.expectedFieldType = expectedFieldType;
            this.intExpectation = intExpectation;
        }

        public RecordExpectation(String fieldName, ExpectedFieldType expectedFieldType, String stringExpectation) {
            this.fieldName = fieldName;
            this.expectedFieldType = expectedFieldType;
            this.stringExpectation = stringExpectation;
        }

        public RecordExpectation(String fieldName, ExpectedFieldType expectedFieldType, double doubleExpectation) {
            this.fieldName = fieldName;
            this.expectedFieldType = expectedFieldType;
            this.doubleExpectation = doubleExpectation;
        }
    }

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
     * Tests whether or not a super simple 2-stop, 1-route, 1-trip, valid gtfs can be loaded
     */
    @Test
    public void canLoadSimpleAgency() {
        assertThat(
            runIntegrationTest(
                "fake-agency",
                nullValue(),
                new PersistanceExpectation[]{
                    new PersistanceExpectation(
                        "agency",
                        new RecordExpectation[]{
                            new RecordExpectation("agency_id", ExpectedFieldType.STRING, "1"),
                            new RecordExpectation("agency_name", ExpectedFieldType.STRING, "Fake Transit"),
                            new RecordExpectation("agency_timezone", ExpectedFieldType.STRING, "America/Los_Angeles")
                        }
                    ),
                    new PersistanceExpectation(
                        "calendar",
                        new RecordExpectation[]{
                            new RecordExpectation(
                                "service_id",
                                ExpectedFieldType.STRING,
                                "04100312-8fe1-46a5-a9f2-556f39478f57"
                            ),
                            new RecordExpectation(
                                "monday",
                                ExpectedFieldType.INT,
                                1
                            ),
                            new RecordExpectation(
                                "tuesday",
                                ExpectedFieldType.INT,
                                1
                            ),
                            new RecordExpectation(
                                "wednesday",
                                ExpectedFieldType.INT,
                                1
                            ),
                            new RecordExpectation(
                                "thursday",
                                ExpectedFieldType.INT,
                                1
                            ),
                            new RecordExpectation(
                                "friday",
                                ExpectedFieldType.INT,
                                1
                            ),
                            new RecordExpectation(
                                "saturday",
                                ExpectedFieldType.INT,
                                1
                            ),
                            new RecordExpectation(
                                "sunday",
                                ExpectedFieldType.INT,
                                1
                            ),
                            new RecordExpectation(
                                "start_date",
                                ExpectedFieldType.STRING,
                                "20170915"
                            ),
                            new RecordExpectation(
                                "end_date",
                                ExpectedFieldType.STRING,
                                "20170917"
                            )
                        }
                    ),
                    new PersistanceExpectation(
                        "calendar_dates",
                        new RecordExpectation[]{
                            new RecordExpectation(
                                "service_id",
                                ExpectedFieldType.STRING,
                                "04100312-8fe1-46a5-a9f2-556f39478f57"
                            ),
                            new RecordExpectation("date", ExpectedFieldType.INT, 20200220),
                            new RecordExpectation("exception_type", ExpectedFieldType.INT, 2)
                        }
                    ),
                    new PersistanceExpectation(
                        "fare_attributes",
                        new RecordExpectation[]{
                            new RecordExpectation("fare_id", ExpectedFieldType.STRING, "route_based_fare"),
                            new RecordExpectation("price", ExpectedFieldType.DOUBLE, 1.23),
                            new RecordExpectation("currency_type", ExpectedFieldType.STRING, "USD")
                        }
                    ),
                    new PersistanceExpectation(
                        "fare_rules",
                        new RecordExpectation[]{
                            new RecordExpectation("fare_id", ExpectedFieldType.STRING, "route_based_fare"),
                            new RecordExpectation("route_id", ExpectedFieldType.STRING, "1")
                        }
                    ),
                    new PersistanceExpectation(
                        "feed_info",
                        new RecordExpectation[]{
                            new RecordExpectation(
                                "feed_publisher_name",
                                ExpectedFieldType.STRING,
                                "Conveyal"
                            ),
                            new RecordExpectation(
                                "feed_publisher_url",
                                ExpectedFieldType.STRING,
                                "http://www.conveyal.com"
                            ),
                            new RecordExpectation(
                                "feed_lang",
                                ExpectedFieldType.STRING,
                                "en"
                            ),
                            new RecordExpectation(
                                "feed_version",
                                ExpectedFieldType.STRING,
                                "1.0"
                            )
                        }
                    ),
                    new PersistanceExpectation(
                        "frequencies",
                        new RecordExpectation[]{
                            new RecordExpectation(
                                "trip_id",
                                ExpectedFieldType.STRING,
                                "frequency-trip"
                            ),
                            new RecordExpectation(
                                "start_time",
                                ExpectedFieldType.INT,
                                28800
                            ),
                            new RecordExpectation(
                                "end_time",
                                ExpectedFieldType.INT,
                                32400
                            ),
                            new RecordExpectation(
                                "headway_secs",
                                ExpectedFieldType.INT,
                                1800
                            ),
                            new RecordExpectation(
                                "exact_times",
                                ExpectedFieldType.INT,
                                0
                            )
                        }
                    ),
                    new PersistanceExpectation(
                        "routes",
                        new RecordExpectation[]{
                            new RecordExpectation(
                                "agency_id",
                                ExpectedFieldType.STRING,
                                "1"
                            ),
                            new RecordExpectation(
                                "route_id",
                                ExpectedFieldType.STRING,
                                "1"
                            ),
                            new RecordExpectation(
                                "route_short_name",
                                ExpectedFieldType.STRING,
                                "1"
                            ),
                            new RecordExpectation(
                                "route_long_name",
                                ExpectedFieldType.STRING,
                                "Route 1"
                            ),
                            new RecordExpectation(
                                "route_type",
                                ExpectedFieldType.INT,
                                3
                            ),
                            new RecordExpectation(
                                "route_color",
                                ExpectedFieldType.STRING,
                                "7CE6E7"
                            )
                        }
                    ),
                    new PersistanceExpectation(
                        "shapes",
                        new RecordExpectation[]{
                            new RecordExpectation(
                                "shape_id",
                                ExpectedFieldType.STRING,
                                "5820f377-f947-4728-ac29-ac0102cbc34e"
                            ),
                            new RecordExpectation(
                                "shape_pt_lat",
                                ExpectedFieldType.DOUBLE,
                                37.061172
                            ),
                            new RecordExpectation(
                                "shape_pt_lon",
                                ExpectedFieldType.DOUBLE,
                                -122.0075
                            ),
                            new RecordExpectation(
                                "shape_pt_sequence",
                                ExpectedFieldType.INT,
                                2
                            ),
                            new RecordExpectation(
                                "shape_dist_traveled",
                                ExpectedFieldType.DOUBLE,
                                7.4997067
                            )
                        }
                    ),
                    new PersistanceExpectation(
                        "stop_times",
                        new RecordExpectation[]{
                            new RecordExpectation(
                                "trip_id",
                                ExpectedFieldType.STRING,
                                "a30277f8-e50a-4a85-9141-b1e0da9d429d"
                            ),
                            new RecordExpectation(
                                "arrival_time",
                                ExpectedFieldType.INT,
                                25200
                            ),
                            new RecordExpectation(
                                "departure_time",
                                ExpectedFieldType.INT,
                                25200
                            ),
                            new RecordExpectation(
                                "stop_id",
                                ExpectedFieldType.STRING,
                                "4u6g"
                            ),
                            new RecordExpectation(
                                "stop_sequence",
                                ExpectedFieldType.INT,
                                1
                            ),
                            new RecordExpectation(
                                "pickup_type",
                                ExpectedFieldType.INT,
                                0
                            ),
                            new RecordExpectation(
                                "drop_off_type",
                                ExpectedFieldType.INT,
                                0
                            ),
                            new RecordExpectation(
                                "shape_dist_traveled",
                                ExpectedFieldType.DOUBLE,
                                0.0
                            )
                        }
                    ),
                    new PersistanceExpectation(
                        "trips",
                        new RecordExpectation[]{
                            new RecordExpectation(
                                "trip_id",
                                ExpectedFieldType.STRING,
                                "a30277f8-e50a-4a85-9141-b1e0da9d429d"
                            ),
                            new RecordExpectation(
                                "service_id",
                                ExpectedFieldType.STRING,
                                "04100312-8fe1-46a5-a9f2-556f39478f57"
                            ),
                            new RecordExpectation(
                                "route_id",
                                ExpectedFieldType.STRING,
                                "1"
                            ),
                            new RecordExpectation(
                                "direction_id",
                                ExpectedFieldType.INT,
                                0
                            ),
                            new RecordExpectation(
                                "shape_id",
                                ExpectedFieldType.STRING,
                                "5820f377-f947-4728-ac29-ac0102cbc34e"
                            ),
                            new RecordExpectation(
                                "bikes_allowed",
                                ExpectedFieldType.INT,
                                0
                            ),
                            new RecordExpectation(
                                "wheelchair_accessible",
                                ExpectedFieldType.INT,
                                0
                            )
                        }
                    )
                }
            ),
            equalTo(true)
        );
    }

    /**
     * A helper method that will run GTFS.main with a certain zip file.
     * This tests whether a GTFS zip file can be loaded without any errors.
     */
    private boolean runIntegrationTest(
        String folderName,
        Matcher<Object> fatalExceptionExpectation,
        PersistanceExpectation[] persistanceExpectations
    ) {
        // zip up test folder into temp zip file
        String zipFileName = null;
        try {
            zipFileName = TestUtils.zipFolderFiles(folderName);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        String newDBName = TestUtils.generateNewDB();
        String dbConnectionUrl = "jdbc:postgresql://localhost/" + newDBName;
        try {
            // load and validate feed
            DataSource dataSource = createDataSource(
                dbConnectionUrl,
                null,
                null
            );
            FeedLoadResult loadResult = load(zipFileName, dataSource);
            ValidationResult validationResult = validate(loadResult.uniqueIdentifier, dataSource);

            assertThat(validationResult.fatalException, is(fatalExceptionExpectation));

            // run through testing expectations
            Connection conn = DriverManager.getConnection(dbConnectionUrl);
            for (PersistanceExpectation persistanceExpectation : persistanceExpectations) {
                // select all entries from a table
                String sql = "select * from " + loadResult.uniqueIdentifier + "." + persistanceExpectation.tableName;
                LOG.info(sql);
                ResultSet rs = conn.prepareStatement(sql).executeQuery();
                boolean foundRecord = false;
                int numRecordsSearched = 0;
                while (rs.next()) {
                    numRecordsSearched++;
                    LOG.info("record " + numRecordsSearched + " in ResultSet");
                    boolean allFieldsMatch = true;
                    for (RecordExpectation recordExpectation: persistanceExpectation.recordExpectations) {
                        switch (recordExpectation.expectedFieldType) {
                            case DOUBLE:
                                LOG.info(recordExpectation.fieldName + ": " + rs.getDouble(recordExpectation.fieldName));
                                if (rs.getDouble(recordExpectation.fieldName) != recordExpectation.doubleExpectation) {
                                    allFieldsMatch = false;
                                }
                                break;
                            case INT:
                                LOG.info(recordExpectation.fieldName + ": " + rs.getInt(recordExpectation.fieldName));
                                if (rs.getInt(recordExpectation.fieldName) != recordExpectation.intExpectation) {
                                    allFieldsMatch = false;
                                }
                                break;
                            case STRING:
                                LOG.info(recordExpectation.fieldName + ": " + rs.getString(recordExpectation.fieldName));
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
                        LOG.info("Record satisfies expectations.");
                        foundRecord = true;
                        break;
                    }
                }
                assertThat(
                    "No records found in the ResultSet",
                    numRecordsSearched,
                    ComparatorMatcherBuilder.<Integer>usingNaturalOrdering().greaterThan(0)
                );
                assertThat(
                    "The record as defined in the PersistanceExpectation was not found.",
                    foundRecord,
                    equalTo(true)
                );
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } finally {
            TestUtils.dropDB(newDBName);
        }
        return true;
    }
}
