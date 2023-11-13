package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.TestUtils;
import com.conveyal.gtfs.dto.CalendarDTO;
import com.conveyal.gtfs.dto.CalendarDateDTO;
import com.conveyal.gtfs.dto.FareDTO;
import com.conveyal.gtfs.dto.FareRuleDTO;
import com.conveyal.gtfs.dto.FeedInfoDTO;
import com.conveyal.gtfs.dto.FrequencyDTO;
import com.conveyal.gtfs.dto.PatternDTO;
import com.conveyal.gtfs.dto.PatternStopDTO;
import com.conveyal.gtfs.dto.RouteDTO;
import com.conveyal.gtfs.dto.ScheduleExceptionDTO;
import com.conveyal.gtfs.dto.ShapePointDTO;
import com.conveyal.gtfs.dto.StopDTO;
import com.conveyal.gtfs.dto.StopTimeDTO;
import com.conveyal.gtfs.dto.TripDTO;
import com.conveyal.gtfs.model.ScheduleException;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.util.InvalidNamespaceException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

import static com.conveyal.gtfs.GTFS.load;
import static com.conveyal.gtfs.GTFS.makeSnapshot;
import static com.conveyal.gtfs.GTFS.validate;
import static com.conveyal.gtfs.TestUtils.getResourceFileName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * This class contains CRUD tests for {@link JdbcTableWriter} (i.e., editing GTFS entities in the RDBMS). Set up
 * consists of creating a scratch database and an empty feed snapshot, which is the necessary starting condition
 * for building a GTFS feed from scratch. It then runs the various CRUD tests and finishes by dropping the database
 * (even if tests fail).
 */
public class JDBCTableWriterTest {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCTableWriterTest.class);

    private static String testDBName;
    private static DataSource testDataSource;
    private static String testNamespace;
    private static String testGtfsGLSnapshotNamespace;
    private static String simpleServiceId = "1";
    private static String firstStopId = "1";
    private static String secondStopId= "1.5";
    private static String lastStopId = "2";
    private static double firstStopLat = 34.2222;
    private static double firstStopLon = -87.333;
    private static double secondStopLat = 34.2227;
    private static double secondStopLon = -87.3335;
    private static double lastStopLat = 34.2233;
    private static double lastStopLon = -87.334;
    private static String sharedShapeId = "shared_shape_id";
    private static final ObjectMapper mapper = new ObjectMapper();

    private static JdbcTableWriter createTestTableWriter (Table table) throws InvalidNamespaceException {
        return new JdbcTableWriter(table, testDataSource, testNamespace);
    }

    @BeforeAll
    public static void setUpClass() throws SQLException, IOException, InvalidNamespaceException {
        // Create a new database
        testDBName = TestUtils.generateNewDB();
        String dbConnectionUrl = String.format("jdbc:postgresql://localhost/%s", testDBName);
        testDataSource = TestUtils.createTestDataSource(dbConnectionUrl);
        LOG.info("creating feeds table because it isn't automatically generated unless you import a feed");
        Connection connection = testDataSource.getConnection();
        connection.createStatement().execute(JdbcGtfsLoader.getCreateFeedRegistrySQL());
        connection.commit();
        LOG.info("feeds table created");
        // Create an empty snapshot to create a new namespace and all the tables
        FeedLoadResult result = makeSnapshot(null, testDataSource, false);
        testNamespace = result.uniqueIdentifier;
        // Create a service calendar and two stops, both of which are necessary to perform pattern and trip tests.
        createWeekdayCalendar(simpleServiceId, "20180103", "20180104");
        createSimpleStop(firstStopId, "First Stop", firstStopLat, firstStopLon);
        createSimpleStop(secondStopId, "Second Stop", secondStopLat, secondStopLon);
        createSimpleStop(lastStopId, "Last Stop", lastStopLat, lastStopLon);

        /** Load the following real-life GTFS for use with {@link JDBCTableWriterTest#canUpdateServiceId()}  **/
        // load feed into db
        FeedLoadResult feedLoadResult = load(getResourceFileName("real-world-gtfs-feeds/gtfs_GL.zip"), testDataSource);
        String testGtfsGLNamespace = feedLoadResult.uniqueIdentifier;
        // validate feed to create additional tables
        validate(testGtfsGLNamespace, testDataSource);
        // load into editor via snapshot
        JdbcGtfsSnapshotter snapshotter = new JdbcGtfsSnapshotter(testGtfsGLNamespace, testDataSource, false);
        SnapshotResult snapshotResult = snapshotter.copyTables();
        testGtfsGLSnapshotNamespace = snapshotResult.uniqueIdentifier;
    }

    @AfterAll
    public static void tearDownClass() {
        TestUtils.dropDB(testDBName);
    }

    @Test
    public void canCreateUpdateAndDeleteFeedInfoEntities() throws IOException, SQLException, InvalidNamespaceException {
        // Store Table and Class values for use in test.
        final Table feedInfoTable = Table.FEED_INFO;
        final Class<FeedInfoDTO> feedInfoDTOClass = FeedInfoDTO.class;

        // create new object to be saved
        FeedInfoDTO feedInfoInput = new FeedInfoDTO();
        String publisherName = "test-publisher";
        feedInfoInput.feed_id = null;
        feedInfoInput.feed_publisher_name = publisherName;
        feedInfoInput.feed_publisher_url = "example.com";
        feedInfoInput.feed_lang = "en";
        feedInfoInput.default_route_color = "1c8edb";
        feedInfoInput.default_route_type = "3";

        // convert object to json and save it
        JdbcTableWriter createTableWriter = createTestTableWriter(feedInfoTable);
        String createOutput = createTableWriter.create(mapper.writeValueAsString(feedInfoInput), true);
        LOG.info("create {} output:", feedInfoTable.name);
        LOG.info(createOutput);

        // parse output
        FeedInfoDTO createdFeedInfo = mapper.readValue(createOutput, feedInfoDTOClass);

        // make sure saved data matches expected data
        assertThat(createdFeedInfo.feed_publisher_name, equalTo(publisherName));

        // try to update record
        String updatedPublisherName = "test-publisher-updated";
        createdFeedInfo.feed_publisher_name = updatedPublisherName;

        // covert object to json and save it
        JdbcTableWriter updateTableWriter = createTestTableWriter(feedInfoTable);
        String updateOutput = updateTableWriter.update(
            createdFeedInfo.id,
            mapper.writeValueAsString(createdFeedInfo),
            true
        );
        LOG.info("update {} output:", feedInfoTable.name);
        LOG.info(updateOutput);

        FeedInfoDTO updatedFeedInfoDTO = mapper.readValue(updateOutput, feedInfoDTOClass);

        // make sure saved data matches expected data
        assertThat(updatedFeedInfoDTO.feed_publisher_name, equalTo(updatedPublisherName));

        // try to delete record
        JdbcTableWriter deleteTableWriter = createTestTableWriter(feedInfoTable);
        int deleteOutput = deleteTableWriter.delete(
            createdFeedInfo.id,
            true
        );
        LOG.info("deleted {} records from {}", deleteOutput, feedInfoTable.name);

        // make sure record does not exist in DB
        assertThatSqlQueryYieldsZeroRows(getColumnsForId(createdFeedInfo.id, feedInfoTable));
    }

    /**
     * Ensure that potentially malicious SQL injection is sanitized properly during create operations.
     * TODO: We might should perform this check on multiple entities and for update and/or delete operations.
     */
    @Test
    public void canPreventSQLInjection() throws IOException, SQLException, InvalidNamespaceException {
        // create new object to be saved
        FeedInfoDTO feedInfoInput = new FeedInfoDTO();
        String publisherName = "' OR 1 = 1; SELECT '1";
        feedInfoInput.feed_id = "fake_id";
        feedInfoInput.feed_publisher_name = publisherName;
        feedInfoInput.feed_publisher_url = "example.com";
        feedInfoInput.feed_lang = "en";
        feedInfoInput.feed_start_date = "07052021";
        feedInfoInput.feed_end_date = "09052021";
        feedInfoInput.feed_lang = "en";
        feedInfoInput.default_route_color = "1c8edb";
        feedInfoInput.default_route_type = "3";
        feedInfoInput.default_lang = "en";
        feedInfoInput.feed_contact_email = "a@b.com";
        feedInfoInput.feed_contact_url = "example.com";

        // convert object to json and save it
        JdbcTableWriter createTableWriter = createTestTableWriter(Table.FEED_INFO);
        String createOutput = createTableWriter.create(mapper.writeValueAsString(feedInfoInput), true);
        LOG.info("create output:");
        LOG.info(createOutput);

        // parse output
        FeedInfoDTO createdFeedInfo = mapper.readValue(createOutput, FeedInfoDTO.class);

        // make sure saved data matches expected data
        assertThat(createdFeedInfo.feed_publisher_name, equalTo(publisherName));
    }

    @Test
    public void canCreateUpdateAndDeleteFares() throws IOException, SQLException, InvalidNamespaceException {
        // Store Table and Class values for use in test.
        final Table fareTable = Table.FARE_ATTRIBUTES;
        final Class<FareDTO> fareDTOClass = FareDTO.class;

        // create new object to be saved
        FareDTO fareInput = new FareDTO();
        String fareId = "2A";
        fareInput.fare_id = fareId;
        fareInput.currency_type = "USD";
        fareInput.price = 2.50;
        fareInput.agency_id = "RTA";
        fareInput.payment_method = 0;
        // Empty string value or null should be permitted for transfers and transfer_duration
        fareInput.transfers = "";
        fareInput.transfer_duration = null;
        FareRuleDTO fareRuleInput = new FareRuleDTO();
        // Fare ID should be assigned to "child entity" by editor automatically.
        fareRuleInput.fare_id = null;
        fareRuleInput.route_id = null;
        // FIXME There is currently no check for valid zone_id values in contains_id, origin_id, and destination_id.
        fareRuleInput.contains_id = "any";
        fareRuleInput.origin_id = "value";
        fareRuleInput.destination_id = "permitted";
        fareInput.fare_rules = new FareRuleDTO[]{fareRuleInput};

        // convert object to json and save it
        JdbcTableWriter createTableWriter = createTestTableWriter(fareTable);
        String createOutput = createTableWriter.create(mapper.writeValueAsString(fareInput), true);
        LOG.info("create {} output:", fareTable.name);
        LOG.info(createOutput);

        // parse output
        FareDTO createdFare = mapper.readValue(createOutput, fareDTOClass);

        // make sure saved data matches expected data
        assertThat(createdFare.fare_id, equalTo(fareId));
        assertThat(createdFare.fare_rules[0].fare_id, equalTo(fareId));

        // Ensure transfers value is null to check database integrity.
        ResultSet resultSet = getResultSetForId(createdFare.id, Table.FARE_ATTRIBUTES);
        while (resultSet.next()) {
            // We must match against null value for transfers because the database stored value will
            // not be an empty string, but null.
            assertResultValue(resultSet, "transfers", Matchers.nullValue());
            assertResultValue(resultSet, "fare_id", equalTo(fareInput.fare_id));
            assertResultValue(resultSet, "currency_type", equalTo(fareInput.currency_type));
            assertResultValue(resultSet, "price", equalTo(fareInput.price));
            assertResultValue(resultSet, "agency_id", equalTo(fareInput.agency_id));
            assertResultValue(resultSet, "payment_method", equalTo(fareInput.payment_method));
            assertResultValue(resultSet, "transfer_duration", equalTo(fareInput.transfer_duration));
        }

        // try to update record
        String updatedFareId = "3B";
        createdFare.fare_id = updatedFareId;
        createdFare.transfers = "0";

        // covert object to json and save it
        JdbcTableWriter updateTableWriter = createTestTableWriter(fareTable);
        String updateOutput = updateTableWriter.update(
                createdFare.id,
                mapper.writeValueAsString(createdFare),
                true
        );
        LOG.info("update {} output:", fareTable.name);
        LOG.info(updateOutput);

        FareDTO updatedFareDTO = mapper.readValue(updateOutput, fareDTOClass);

        // make sure saved data matches expected data
        assertThat(updatedFareDTO.fare_id, equalTo(updatedFareId));
        assertThat(updatedFareDTO.fare_rules[0].fare_id, equalTo(updatedFareId));

        // Ensure transfers value is updated correctly to check database integrity.
        ResultSet updatedResult = getResultSetForId(createdFare.id, Table.FARE_ATTRIBUTES);
        while (updatedResult.next()) {
            assertResultValue(updatedResult, "transfers", equalTo(0));
            assertResultValue(updatedResult, "fare_id", equalTo(createdFare.fare_id));
        }

        // try to delete record
        JdbcTableWriter deleteTableWriter = createTestTableWriter(fareTable);
        int deleteOutput = deleteTableWriter.delete(
                createdFare.id,
                true
        );
        LOG.info("deleted {} records from {}", deleteOutput, fareTable.name);

        // make sure fare_attributes record does not exist in DB
        assertThatSqlQueryYieldsZeroRows(getColumnsForId(createdFare.id, fareTable));

        // make sure fare_rules record does not exist in DB
        assertThatSqlQueryYieldsZeroRows(getColumnsForId(createdFare.fare_rules[0].id, Table.FARE_RULES));
    }

    @Test
    public void canCreateUpdateAndDeleteRoutes() throws IOException, SQLException, InvalidNamespaceException {
        // Store Table and Class values for use in test.
        final Table routeTable = Table.ROUTES;
        final Class<RouteDTO> routeDTOClass = RouteDTO.class;

        // create new object to be saved
        String routeId = "500";
        RouteDTO createdRoute = createSimpleTestRoute(routeId, "RTA", "500", "Hollingsworth", 3);
        // Set values to empty strings/null to later verify that they are set to null in the database.
        createdRoute.route_color = "";
        createdRoute.route_sort_order = "";
        // make sure saved data matches expected data
        assertThat(createdRoute.route_id, equalTo(routeId));
        // TODO: Verify with a SQL query that the database now contains the created data (we may need to use the same
        //       db connection to do this successfully?)

        // try to update record
        String updatedRouteId = "600";
        createdRoute.route_id = updatedRouteId;

        // convert object to json and save it
        JdbcTableWriter updateTableWriter = createTestTableWriter(routeTable);
        String updateOutput = updateTableWriter.update(
                createdRoute.id,
                mapper.writeValueAsString(createdRoute),
                true
        );
        LOG.info("update {} output:", routeTable.name);
        LOG.info(updateOutput);

        RouteDTO updatedRouteDTO = mapper.readValue(updateOutput, routeDTOClass);

        // make sure saved data matches expected data
        assertThat(updatedRouteDTO.route_id, equalTo(updatedRouteId));
        // Ensure route_color is null (not empty string).
        LOG.info("route_color: {}", updatedRouteDTO.route_color);
        assertNull(updatedRouteDTO.route_color);
        // Verify that certain values are correctly set in the database.
        ResultSet resultSet = getResultSetForId(updatedRouteDTO.id, routeTable);
        while (resultSet.next()) {
            assertResultValue(resultSet, "route_color", Matchers.nullValue());
            assertResultValue(resultSet, "route_id", equalTo(createdRoute.route_id));
            assertResultValue(resultSet, "route_sort_order", Matchers.nullValue());
            assertResultValue(resultSet, "route_type", equalTo(createdRoute.route_type));
        }
        // try to delete record
        JdbcTableWriter deleteTableWriter = createTestTableWriter(routeTable);
        int deleteOutput = deleteTableWriter.delete(
                createdRoute.id,
                true
        );
        LOG.info("deleted {} records from {}", deleteOutput, routeTable.name);

        // make sure route record does not exist in DB
        assertThatSqlQueryYieldsZeroRows(getColumnsForId(createdRoute.id, routeTable));
    }

    /**
     * Ensure that a simple {@link ScheduleException} can be created, updated, and deleted.
     */
    @Test
    public void canCreateUpdateAndDeleteScheduleExceptions() throws IOException, SQLException, InvalidNamespaceException {
        // Store Table and Class values for use in test.
        final Table scheduleExceptionTable = Table.SCHEDULE_EXCEPTIONS;
        final Class<ScheduleExceptionDTO> scheduleExceptionDTOClass = ScheduleExceptionDTO.class;
        // Create new object to be saved.
        ScheduleExceptionDTO exceptionInput = new ScheduleExceptionDTO();
        exceptionInput.name = "Halloween";
        exceptionInput.exemplar = 9; // Add, swap, or remove type
        exceptionInput.removed_service = new String[]{simpleServiceId};
        String[] halloweenDate = new String[]{"20191031"};
        exceptionInput.dates = halloweenDate;
        TableWriter<ScheduleException> createTableWriter = createTestTableWriter(scheduleExceptionTable);
        String scheduleExceptionOutput = createTableWriter.create(mapper.writeValueAsString(exceptionInput), true);
        ScheduleExceptionDTO scheduleException = mapper.readValue(scheduleExceptionOutput,
                                                                         scheduleExceptionDTOClass);
        // Make sure saved data matches expected data.
        assertThat(scheduleException.removed_service[0], equalTo(simpleServiceId));
        ResultSet resultSet = getResultSetForId(scheduleException.id, scheduleExceptionTable, "removed_service");
        while (resultSet.next()) {
            String[] array = (String[]) resultSet.getArray(1).getArray();
            for (int i = 0; i < array.length; i++) {
                assertEquals(exceptionInput.removed_service[i], array[i]);
            }
        }
        // try to update record
        String[] updatedDates = new String[]{"20191031", "20201031"};
        scheduleException.dates = updatedDates;
        // covert object to json and save it
        JdbcTableWriter updateTableWriter = createTestTableWriter(scheduleExceptionTable);
        String updateOutput = updateTableWriter.update(
            scheduleException.id,
            mapper.writeValueAsString(scheduleException),
            true
        );
        LOG.info("update {} output:", scheduleExceptionTable.name);
        LOG.info(updateOutput);
        ScheduleExceptionDTO updatedDTO = mapper.readValue(updateOutput, scheduleExceptionDTOClass);
        // Make sure saved data matches expected data.
        assertThat(updatedDTO.dates, equalTo(updatedDates));
        ResultSet rs2 = getResultSetForId(scheduleException.id, scheduleExceptionTable, "dates");
        while (rs2.next()) {
            String[] array = (String[]) rs2.getArray(1).getArray();
            for (int i = 0; i < array.length; i++) {
                assertEquals(updatedDates[i], array[i]);
            }
        }
        // try to delete record
        JdbcTableWriter deleteTableWriter = createTestTableWriter(scheduleExceptionTable);
        int deleteOutput = deleteTableWriter.delete(
            scheduleException.id,
            true
        );
        LOG.info("deleted {} records from {}", deleteOutput, scheduleExceptionTable.name);
        // Make sure route record does not exist in DB.
        assertThatSqlQueryYieldsZeroRows(getColumnsForId(scheduleException.id, scheduleExceptionTable));
    }

    /**
     * Ensure that {@link ScheduleException}s which are loaded from an existing GTFS can be removed properly,
     * including created entries in calendar_dates.
     */
    @Test
    public void canCreateAndDeleteCalendarDates() throws IOException, SQLException, InvalidNamespaceException {
        String firstServiceId = "REMOVED";
        String secondServiceId = "ADDED";
        String[] allServiceIds = new String[] {firstServiceId, secondServiceId};
        String[] holidayDates = new String[] {"20190812", "20190813", "20190814"};

        final Table scheduleExceptionTable = Table.SCHEDULE_EXCEPTIONS;
        final Table calendarDatesTable = Table.CALENDAR_DATES;
        final Class<ScheduleExceptionDTO> scheduleExceptionDTOClass = ScheduleExceptionDTO.class;

        // Create new schedule exception which involves 2 service IDs and multiple dates
        ScheduleExceptionDTO exceptionInput = new ScheduleExceptionDTO();
        exceptionInput.name = "Incredible multi day holiday";
        exceptionInput.exemplar = 9; // Add, swap, or remove type
        exceptionInput.removed_service = new String[] {firstServiceId};
        exceptionInput.added_service = new String[] {secondServiceId};
        exceptionInput.dates = holidayDates;

        // Save the schedule exception
        // TODO: share this with other schedule exception method?
        TableWriter<ScheduleException> createTableWriter = createTestTableWriter(scheduleExceptionTable);
        String scheduleExceptionOutput = createTableWriter.create(mapper.writeValueAsString(exceptionInput), true);
        ScheduleExceptionDTO scheduleException = mapper.readValue(scheduleExceptionOutput, scheduleExceptionDTOClass);

        // Create a calendar_dates entry for each date of the schedule exception
        for (String date: holidayDates) {
            createAndStoreCalendarDate(firstServiceId, date, 2); // firstServiceId is removed
            createAndStoreCalendarDate(secondServiceId, date, 1); // secondServiceId is added
        }

        // Delete a schedule exception
        JdbcTableWriter deleteTableWriter = createTestTableWriter(scheduleExceptionTable);
        int deleteOutput = deleteTableWriter.delete(scheduleException.id, true);
        LOG.info("deleted {} records from {}", deleteOutput, scheduleExceptionTable.name);

        // Verify that the entries in calendar_dates are removed after deleting the schedule exception.
        for (String date : holidayDates) {
            for (String serviceId : allServiceIds){
                String sql = String.format("select * from %s.%s where service_id = '%s' and date = '%s'",
                        testNamespace,
                        calendarDatesTable.name,
                        serviceId,
                        date
                );
                assertThatSqlQueryYieldsZeroRows(sql);
            }
        }
    }

    /**
     * This test verifies that stop_times#shape_dist_traveled and other linked fields are updated when a pattern
     * is updated.
     */
    @Test
    public void shouldUpdateStopTimeOnPatternStopUpdate() throws IOException, SQLException, InvalidNamespaceException {
        final String[] STOP_TIMES_LINKED_FIELDS = new String[] {
            "shape_dist_traveled",
            "timepoint",
            "drop_off_type",
            "pickup_type",
            "continuous_pickup",
            "continuous_drop_off"
        };
        String routeId = newUUID();
        String patternId = newUUID();
        int startTime = 6 * 60 * 60; // 6 AM
        PatternDTO pattern = createRouteAndPattern(
            routeId,
            patternId,
            "pattern name",
            null,
            new ShapePointDTO[]{},
            new PatternStopDTO[]{
                new PatternStopDTO(patternId, firstStopId, 0),
                new PatternStopDTO(patternId, lastStopId, 1)
            },
            0
        );
        // Make sure saved data matches expected data.
        assertThat(pattern.route_id, equalTo(routeId));
        // Create trip so we can check that the stop_time values are updated after the patter update.
        TripDTO tripInput = constructTimetableTrip(pattern.pattern_id, pattern.route_id, startTime, 60);
        // Set trip_id to empty string to verify that it gets overwritten with auto-generated UUID.
        tripInput.trip_id = "";
        JdbcTableWriter createTripWriter = createTestTableWriter(Table.TRIPS);
        String createdTripOutput = createTripWriter.create(mapper.writeValueAsString(tripInput), true);
        TripDTO createdTrip = mapper.readValue(createdTripOutput, TripDTO.class);
        // Check that trip_id is not empty.
        assertNotEquals("", createdTrip.trip_id);
        // Check that trip_id is a UUID.
        LOG.info("New trip_id = {}", createdTrip.trip_id);
        UUID uuid = UUID.fromString(createdTrip.trip_id);
        assertNotNull(uuid);
        // Check that trip exists.
        assertThatSqlQueryYieldsRowCount(getColumnsForId(createdTrip.id, Table.TRIPS), 1);

        // Check the stop_time's initial shape_dist_traveled value and other linked fields.
        PreparedStatement statement = testDataSource.getConnection().prepareStatement(
            String.format(
                "select %s from %s.stop_times where stop_sequence=1 and trip_id='%s'",
                String.join(", ", STOP_TIMES_LINKED_FIELDS),
                testNamespace,
                createdTrip.trip_id
            )
        );
        LOG.info(statement.toString());
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
            // First stop_time shape_dist_traveled should be zero.
            // Other linked fields should be interpreted as zero too.
            for (int i = 1; i <= STOP_TIMES_LINKED_FIELDS.length; i++) {
                assertThat(resultSet.getInt(i), equalTo(0));
            }
        }

        // Update pattern_stop#shape_dist_traveled and check that the stop_time's shape_dist value is updated.
        final double updatedShapeDistTraveled = 45.5;
        PatternStopDTO pattern_stop = pattern.pattern_stops[1];
        pattern_stop.shape_dist_traveled = updatedShapeDistTraveled;
        // Assign an arbitrary value (the order of appearance in STOP_TIMES_LINKED_FIELDS) for the other linked fields.
        pattern_stop.timepoint = 2;
        pattern_stop.drop_off_type = 3;
        pattern_stop.pickup_type = 4;
        pattern_stop.continuous_pickup = 5;
        pattern_stop.continuous_drop_off = 6;
        JdbcTableWriter patternUpdater = createTestTableWriter(Table.PATTERNS);
        String updatedPatternOutput = patternUpdater.update(pattern.id, mapper.writeValueAsString(pattern), true);
        LOG.info("Updated pattern: {}", updatedPatternOutput);
        ResultSet resultSet2 = statement.executeQuery();
        while (resultSet2.next()) {
            // First stop_time shape_dist_traveled should be updated.
            assertThat(resultSet2.getDouble(1), equalTo(updatedShapeDistTraveled));

            // Other linked fields should be as set above.
            for (int i = 2; i <= STOP_TIMES_LINKED_FIELDS.length; i++) {
                assertThat(resultSet2.getInt(i), equalTo(i));
            }
        }
    }

    @Test
    public void shouldDeleteReferencingTripsAndStopTimesOnPatternDelete() throws IOException, SQLException, InvalidNamespaceException {
        String routeId = "9834914";
        int startTime = 6 * 60 * 60; // 6 AM
        PatternDTO pattern = createRouteAndSimplePattern(routeId, "9901900", "The Line");
        // make sure saved data matches expected data
        assertThat(pattern.route_id, equalTo(routeId));
        TripDTO tripInput = constructTimetableTrip(pattern.pattern_id, pattern.route_id, startTime, 60);
        JdbcTableWriter createTripWriter = createTestTableWriter(Table.TRIPS);
        String createdTripOutput = createTripWriter.create(mapper.writeValueAsString(tripInput), true);
        TripDTO createdTrip = mapper.readValue(createdTripOutput, TripDTO.class);
        assertThatSqlQueryYieldsRowCount(getColumnsForId(createdTrip.id, Table.TRIPS), 1);
        // Delete pattern record
        JdbcTableWriter deletePatternWriter = createTestTableWriter(Table.PATTERNS);
        int deleteOutput = deletePatternWriter.delete(pattern.id, true);
        LOG.info("deleted {} records from {}", deleteOutput, Table.PATTERNS.name);
        // Check that pattern record does not exist in DB
        assertThatSqlQueryYieldsZeroRows(getColumnsForId(pattern.id, Table.PATTERNS));
        // Check that trip records for pattern do not exist in DB
        assertThatSqlQueryYieldsZeroRows(
            String.format(
                "select * from %s.%s where pattern_id='%s'",
                testNamespace,
                Table.TRIPS.name,
                pattern.pattern_id
            ));
        // Check that stop_times records for trip do not exist in DB
        assertThatSqlQueryYieldsZeroRows(
            String.format(
                "select * from %s.%s where trip_id='%s'",
                testNamespace,
                Table.STOP_TIMES.name,
                createdTrip.trip_id
            ));
    }

    /**
     * Deleting a route should also delete related shapes because they are unique to this route.
     */
    @Test
    void shouldDeleteRouteShapes() throws IOException, SQLException, InvalidNamespaceException {
        String routeId = "8472017";
        String shapeId = "uniqueShapeId";

        createThenDeleteRoute(routeId, shapeId);

        // Check that shape records for pattern do not exist in DB
        assertThatSqlQueryYieldsZeroRows(
            String.format(
                "select * from %s where shape_id = '%s'",
                String.format("%s.%s", testNamespace, Table.SHAPES.name),
                shapeId
            ));
    }

    /**
     * Deleting a route should retain shapes that are shared by multiple patterns.
     */
    @Test
    void shouldRetainSharedShapes() throws IOException, SQLException, InvalidNamespaceException {
        String routeId = "8472017";
        String shapeId = "sharedShapeId";

        // Create a pattern which uses the same shape as the pattern that will be deleted. This is to prevent the shape
        // from being deleted.
        createSimplePattern("111222", "8802800", "The Line", shapeId);

        createThenDeleteRoute(routeId, shapeId);

        // Check that shape records persist in DB
        assertThatSqlQueryYieldsRowCount(
            String.format(
                "select * from %s where shape_id = '%s'",
                String.format("%s.%s", testNamespace, Table.SHAPES.name),
                shapeId
            ), 4); // Two shapes are created per pattern. Two patterns equals four shapes.
    }

    /**
     * Create a route with related pattern, trip and stop times. Confirm entities have been created successfully, then
     * delete the route to trigger cascade deleting of patterns, trips, stop times and shapes.
     *
     */
    private void createThenDeleteRoute(String routeId, String shapeId)
        throws InvalidNamespaceException, SQLException, IOException {

        int startTime = 6 * 60 * 60; // 6 AM

        RouteDTO createdRoute = createSimpleTestRoute(routeId, "RTA", "500", "Hollingsworth", 3);
        PatternDTO pattern = createSimplePattern(routeId, "9901900", "The Line", shapeId);
        // Make sure saved data matches expected data.
        assertThat(pattern.route_id, equalTo(routeId));

        TripDTO tripInput = constructFrequencyTrip(pattern.pattern_id, pattern.route_id, startTime);
        JdbcTableWriter createTripWriter = createTestTableWriter(Table.TRIPS);
        String createdTripOutput = createTripWriter.create(mapper.writeValueAsString(tripInput), true);
        TripDTO createdTrip = mapper.readValue(createdTripOutput, TripDTO.class);
        assertThatSqlQueryYieldsRowCount(getColumnsForId(createdTrip.id, Table.TRIPS), 1);

        // Delete route record
        JdbcTableWriter deleteRouteWriter = createTestTableWriter(Table.ROUTES);
        int deleteOutput = deleteRouteWriter.delete(createdRoute.id, true);
        LOG.info("deleted {} records from {}", deleteOutput, Table.ROUTES.name);

        confirmRemovalOfRouteRelatedData(pattern.id, pattern.pattern_id, createdTrip.trip_id);
    }

    /**
     * Confirm that items related to a route no longer exist after a cascade delete.
     */
    private void confirmRemovalOfRouteRelatedData(Integer id, String patternId, String tripId) throws SQLException {
        // Check that pattern record does not exist in DB
        assertThatSqlQueryYieldsZeroRows(getColumnsForId(id, Table.PATTERNS));

        // Check that trip records for pattern do not exist in DB
        assertThatSqlQueryYieldsZeroRows(
            String.format(
                "select * from %s.%s where pattern_id='%s'",
                testNamespace,
                Table.TRIPS.name,
                patternId
            )
        );

        // Check that stop_times records for trip do not exist in DB
        assertThatSqlQueryYieldsZeroRows(
            String.format(
                "select * from %s.%s where trip_id='%s'",
                testNamespace,
                Table.STOP_TIMES.name,
                tripId
            )
        );

        // Check that pattern_stops records for pattern do not exist in DB
        assertThatSqlQueryYieldsZeroRows(
            String.format(
                "select * from %s.%s where pattern_id='%s'",
                testNamespace,
                Table.PATTERN_STOP.name,
                patternId
            )
        );

        // Check that frequency records for trip do not exist in DB
        assertThatSqlQueryYieldsZeroRows(
            String.format(
                "select * from %s.%s where trip_id='%s'",
                testNamespace,
                Table.FREQUENCIES.name,
                tripId
            )
        );
    }

    /**
     * Test that a frequency trip entry CANNOT be added for a timetable-based pattern. Expects an exception to be thrown.
     */
    @Test
    public void cannotCreateFrequencyForTimetablePattern() {
        Assertions.assertThrows(IllegalStateException.class, () -> {
            PatternDTO simplePattern = createRouteAndSimplePattern("900", "8", "The Loop");
            TripDTO tripInput = constructFrequencyTrip(simplePattern.pattern_id, simplePattern.route_id, 6 * 60 * 60);
            JdbcTableWriter createTripWriter = createTestTableWriter(Table.TRIPS);
            createTripWriter.create(mapper.writeValueAsString(tripInput), true);
        });
    }

    /**
     * When multiple patterns reference a single shape_id, the returned JSON from an update to any of these patterns
     * (whether the shape points were updated or not) should have a new shape_id because of the "copy on update" logic
     * that ensures the shared shape is not modified.
     */
    @Test
    public void shouldChangeShapeIdOnPatternUpdate() throws IOException, SQLException, InvalidNamespaceException {
        String patternId = "10";
        ShapePointDTO[] shapes = new ShapePointDTO[]{
            new ShapePointDTO(2, 0.0, sharedShapeId, firstStopLat, firstStopLon, 0),
            new ShapePointDTO(2, 150.0, sharedShapeId, lastStopLat, lastStopLon, 1)
        };
        PatternStopDTO[] patternStops = new PatternStopDTO[]{
            new PatternStopDTO(patternId, firstStopId, 0),
            new PatternStopDTO(patternId, lastStopId, 1)
        };
        PatternDTO simplePattern = createRouteAndPattern("1001", patternId, "The Line", sharedShapeId, shapes, patternStops, 0);
        assertThat(simplePattern.shape_id, equalTo(sharedShapeId));
        // Create pattern with shared shape. Note: typically we would encounter shared shapes on imported feeds (e.g.,
        // BART), but this should simulate the situation well enough.
        String secondPatternId = "11";
        patternStops[0].pattern_id = secondPatternId;
        patternStops[1].pattern_id = secondPatternId;
        PatternDTO patternWithSharedShape = createRouteAndPattern("1002", secondPatternId, "The Line 2", sharedShapeId, shapes, patternStops, 0);
        // Verify that shape_id is shared.
        assertThat(patternWithSharedShape.shape_id, equalTo(sharedShapeId));
        // Update any field on one of the patterns.
        JdbcTableWriter patternUpdater = createTestTableWriter(Table.PATTERNS);
        patternWithSharedShape.name = "The shape_id should update";
        String sharedPatternOutput = patternUpdater.update(patternWithSharedShape.id, mapper.writeValueAsString(patternWithSharedShape), true);
        // The output should contain a new backend-generated shape_id.
        PatternDTO updatedSharedPattern = mapper.readValue(sharedPatternOutput, PatternDTO.class);
        LOG.info("Updated pattern output: {}", sharedPatternOutput);
        String newShapeId = updatedSharedPattern.shape_id;
        assertThat(newShapeId, not(equalTo(sharedShapeId)));
        // Ensure that pattern record in database reflects updated shape ID.
        assertThatSqlQueryYieldsRowCount(String.format(
            "select * from %s.%s where shape_id='%s' and pattern_id='%s'",
            testNamespace,
            Table.PATTERNS.name,
            newShapeId,
            secondPatternId
        ), 1);
    }

    /**
     * Checks that creating a frequency trip functions properly. This also updates the pattern to include pattern stops,
     * which is a prerequisite for creating a frequency trip with stop times.
     */
    @Test
    public void canCreateUpdateAndDeleteFrequencyTripForFrequencyPattern() throws IOException, SQLException, InvalidNamespaceException {
        // Store Table and Class values for use in test.
        final Table tripsTable = Table.TRIPS;
        int startTime = 6 * 60 * 60;
        PatternDTO simplePattern = createRouteAndSimplePattern("1000", "9", "The Line");
        TripDTO tripInput = constructFrequencyTrip(simplePattern.pattern_id, simplePattern.route_id, startTime);
        JdbcTableWriter createTripWriter = createTestTableWriter(tripsTable);
        // Update pattern with pattern stops, set to use frequencies, and TODO shape points
        JdbcTableWriter patternUpdater = createTestTableWriter(Table.PATTERNS);
        simplePattern.use_frequency = 1;
        simplePattern.pattern_stops = new PatternStopDTO[]{
            new PatternStopDTO(simplePattern.pattern_id, firstStopId, 0),
            new PatternStopDTO(simplePattern.pattern_id, lastStopId, 1)
        };
        String updatedPatternOutput = patternUpdater.update(simplePattern.id, mapper.writeValueAsString(simplePattern), true);
        LOG.info("Updated pattern output: {}", updatedPatternOutput);
        // Create new trip for the pattern
        String createTripOutput = createTripWriter.create(mapper.writeValueAsString(tripInput), true);
        LOG.info(createTripOutput);
        TripDTO createdTrip = mapper.readValue(createTripOutput, TripDTO.class);
        // Update trip
        // TODO: Add update and delete tests for updating pattern stops, stop_times, and frequencies.
        String updatedTripId = "100A";
        createdTrip.trip_id = updatedTripId;
        JdbcTableWriter updateTripWriter = createTestTableWriter(tripsTable);
        String updateTripOutput = updateTripWriter.update(createdTrip.id, mapper.writeValueAsString(createdTrip), true);
        LOG.info(updateTripOutput);
        TripDTO updatedTrip = mapper.readValue(updateTripOutput, TripDTO.class);
        // Check that saved data matches expected data
        assertThat(updatedTrip.frequencies[0].start_time, equalTo(startTime));
        assertThat(updatedTrip.trip_id, equalTo(updatedTripId));
        // Delete trip record
        JdbcTableWriter deleteTripWriter = createTestTableWriter(tripsTable);
        int deleteOutput = deleteTripWriter.delete(createdTrip.id, true);
        LOG.info("deleted {} records from {}", deleteOutput, tripsTable.name);
        // Check that trip record does not exist in DB
        assertThatSqlQueryYieldsZeroRows(
            String.format(
                "select * from %s.%s where id=%d",
                testNamespace,
                tripsTable.name,
                updatedTrip.id
            ));
        // Check that stop_times records do not exist in DB
        assertThatSqlQueryYieldsZeroRows(
            String.format(
                "select * from %s.%s where trip_id='%s'",
                testNamespace,
                Table.STOP_TIMES.name,
                updatedTrip.trip_id
            ));
    }

    private static String normalizeStopsForPattern(PatternStopDTO[] patternStops, int updatedStopSequence, boolean interpolateStopTimes, int initialTravelTime, int updatedTravelTime, int startTime, String patternId) throws SQLException, InvalidNamespaceException, IOException {
        final Table tripsTable = Table.TRIPS;

        PatternDTO pattern = createRouteAndPattern(newUUID(),
            patternId,
            "Pattern A",
            null,
            new ShapePointDTO[]{},
            patternStops,
            0);

        // Create trip with travel times that match pattern stops.
        TripDTO tripInput = constructTimetableTrip(pattern.pattern_id, pattern.route_id, startTime, initialTravelTime);
        JdbcTableWriter createTripWriter = createTestTableWriter(tripsTable);
        String createTripOutput = createTripWriter.create(mapper.writeValueAsString(tripInput), true);
        LOG.info(createTripOutput);
        TripDTO createdTrip = mapper.readValue(createTripOutput, TripDTO.class);
        // Update pattern stop with new travel time.
        JdbcTableWriter patternUpdater = createTestTableWriter(Table.PATTERNS);
        pattern.pattern_stops[updatedStopSequence].default_travel_time = updatedTravelTime;
        String updatedPatternOutput = patternUpdater.update(pattern.id, mapper.writeValueAsString(pattern), true);
        LOG.info("Updated pattern output: {}", updatedPatternOutput);
        // Normalize stop times.
        JdbcTableWriter updateTripWriter = createTestTableWriter(tripsTable);
        updateTripWriter.normalizeStopTimesForPattern(pattern.id, 0, interpolateStopTimes);

        return createdTrip.trip_id;
    }

    /**
     * Checks that {@link JdbcTableWriter#normalizeStopTimesForPattern(int, int, boolean)} can interpolate stop times between timepoints.
     */
    @Test
    public void canInterpolatePatternStopTimes() throws IOException, SQLException, InvalidNamespaceException {
        // Parameters are shared with canNormalizePatternStopTimes, but maintained for test flexibility.
        int startTime = 6 * 60 * 60; // 6AM
        int initialTravelTime = 60; // seconds
        int updatedTravelTime = 600; // ten minutes
        String patternId = "123456-interpolated";
        double[] shapeDistTraveledValues = new double[] {0.0, 300.0, 600.0};
        double timepointTravelTime = (shapeDistTraveledValues[2] - shapeDistTraveledValues[0]) / updatedTravelTime; // 1 m/s

        // Create the array of patterns, set the timepoints properly.
        PatternStopDTO[] patternStops = new PatternStopDTO[]{
            new PatternStopDTO(patternId, firstStopId, 0, 1, shapeDistTraveledValues[0]),
            new PatternStopDTO(patternId, secondStopId, 1, 0, shapeDistTraveledValues[1]),
            new PatternStopDTO(patternId, lastStopId, 2, 1, shapeDistTraveledValues[2]),
        };

        patternStops[2].default_travel_time = initialTravelTime;

        // Pass the array of patterns to the body method with param
        String createdTripId = normalizeStopsForPattern(patternStops, 2, true, initialTravelTime, updatedTravelTime, startTime, patternId);

        // Read pattern stops from database and check that the arrivals/departures have been updated.
        JDBCTableReader<StopTime> stopTimesTable = new JDBCTableReader(Table.STOP_TIMES,
            testDataSource,
            testNamespace + ".",
            EntityPopulator.STOP_TIME);
        int index = 0;
        for (StopTime stopTime : stopTimesTable.getOrdered(createdTripId)) {
            LOG.info("stop times i={} arrival={} departure={}", index, stopTime.arrival_time, stopTime.departure_time);
            int calculatedArrivalTime = (int) (startTime + shapeDistTraveledValues[index] * timepointTravelTime);
            assertThat(stopTime.arrival_time, equalTo(calculatedArrivalTime));
            index++;
        }
    }

    /**
     * Checks that {@link JdbcTableWriter#normalizeStopTimesForPattern(int, int, boolean)} can normalize stop times to a pattern's
     * default travel times.
     */
    @Test
    public void canNormalizePatternStopTimes() throws IOException, SQLException, InvalidNamespaceException {
        // Parameters are shared with canNormalizePatternStopTimes, but maintained for test flexibility.
        int initialTravelTime = 60; // one minute
        int startTime = 6 * 60 * 60; // 6AM
        int updatedTravelTime = 3600;
        String patternId = "123456";

        PatternStopDTO[] patternStops = new PatternStopDTO[]{
            new PatternStopDTO(patternId, firstStopId, 0),
            new PatternStopDTO(patternId, lastStopId, 1)
        };

        String createdTripId = normalizeStopsForPattern(patternStops, 1, false, initialTravelTime, updatedTravelTime, startTime, patternId);
        JDBCTableReader<StopTime> stopTimesTable = new JDBCTableReader(Table.STOP_TIMES,
            testDataSource,
            testNamespace + ".",
            EntityPopulator.STOP_TIME);
        int index = 0;
        for (StopTime stopTime : stopTimesTable.getOrdered(createdTripId)) {
            LOG.info("stop times i={} arrival={} departure={}", index, stopTime.arrival_time, stopTime.departure_time);
            assertThat(stopTime.arrival_time, equalTo(startTime + index * updatedTravelTime));
            index++;
        }
        // Ensure that updated stop times equals pattern stops length
        assertThat(index, equalTo(patternStops.length));
    }

    /**
     * This test makes sure that updated the service_id will properly update affected referenced entities properly.
     * This test case was initially developed to prove that https://github.com/conveyal/gtfs-lib/issues/203 is
     * happening.
     */
    @Test
    public void canUpdateServiceId() throws InvalidNamespaceException, IOException, SQLException {
        // change the service id
        JdbcTableWriter tableWriter = new JdbcTableWriter(Table.CALENDAR, testDataSource, testGtfsGLSnapshotNamespace);
        tableWriter.update(
            2,
            "{\"id\":2,\"service_id\":\"test\",\"description\":\"MoTuWeThFrSaSu\",\"monday\":1,\"tuesday\":1,\"wednesday\":1,\"thursday\":1,\"friday\":1,\"saturday\":1,\"sunday\":1,\"start_date\":\"20180526\",\"end_date\":\"20201231\"}",
            true
        );

        // assert that the amount of stop times equals the original amount of stop times in the feed
        assertThatSqlQueryYieldsRowCount(
            String.format(
                "select * from %s.%s",
                testGtfsGLSnapshotNamespace,
                Table.STOP_TIMES.name
            ),
            53
        );
    }

    /*****************************************************************************************************************
     * End tests, begin helpers
     ****************************************************************************************************************/

    private static String newUUID() {
        return UUID.randomUUID().toString();
    }

    /**
     * Constructs SQL query for the specified ID and columns and returns the resulting result set.
     */
    private String getColumnsForId(int id, Table table, String... columns) {
        String sql = String.format(
            "select %s from %s.%s where id=%d",
            columns.length > 0 ? String.join(", ", columns) : "*",
            testNamespace,
            table.name,
            id
        );
        LOG.info(sql);
        return sql;
    }

    /**
     * Executes SQL query for the specified ID and columns and returns the resulting result set.
     */
    private ResultSet getResultSetForId(int id, Table table, String... columns) throws SQLException {
        String sql = getColumnsForId(id, table, columns);
        return testDataSource.getConnection().prepareStatement(sql).executeQuery();
    }

    /**
     * Asserts that a given value for the specified field in result set matches provided matcher.
     */
    private void assertResultValue(ResultSet resultSet, String field, Matcher matcher) throws SQLException {
        assertThat(resultSet.getObject(field), matcher);
    }

    private void assertThatSqlQueryYieldsRowCount(String sql, int expectedRowCount) throws SQLException {
        LOG.info(sql);
        int recordCount = 0;
        ResultSet rs = testDataSource.getConnection().prepareStatement(sql).executeQuery();
        while (rs.next()) recordCount++;
        assertThat("Records matching query should equal expected count.", recordCount, equalTo(expectedRowCount));
    }

    void assertThatSqlQueryYieldsZeroRows(String sql) throws SQLException {
        assertThatSqlQueryYieldsRowCount(sql, 0);
    }

    /**
     * Construct (without writing to the database) a trip with a frequency entry.
     */
    private TripDTO constructFrequencyTrip(String patternId, String routeId, int startTime) {
        TripDTO tripInput = new TripDTO();
        tripInput.pattern_id = patternId;
        tripInput.route_id = routeId;
        tripInput.service_id = simpleServiceId;
        tripInput.stop_times = new StopTimeDTO[]{
            new StopTimeDTO(firstStopId, 0, 0, 0),
            new StopTimeDTO(lastStopId, 60, 60, 1)
        };
        FrequencyDTO frequency = new FrequencyDTO();
        frequency.start_time = startTime;
        frequency.end_time = 9 * 60 * 60;
        frequency.headway_secs = 15 * 60;
        tripInput.frequencies = new FrequencyDTO[]{frequency};
        return tripInput;
    }

    /**
     * Construct (without writing to the database) a timetable trip.
     */
    private static TripDTO constructTimetableTrip(String patternId, String routeId, int startTime, int travelTime) {
        TripDTO tripInput = new TripDTO();
        tripInput.pattern_id = patternId;
        tripInput.route_id = routeId;
        tripInput.service_id = simpleServiceId;
        tripInput.stop_times = new StopTimeDTO[]{
            new StopTimeDTO(firstStopId, startTime, startTime, 0),
            new StopTimeDTO(lastStopId, startTime + travelTime, startTime + travelTime, 1)
        };
        tripInput.frequencies = new FrequencyDTO[]{};
        return tripInput;
    }

    /**
     * Creates a pattern by first creating a route and then a pattern for that route.
     */
    private static PatternDTO createRouteAndPattern(String routeId, String patternId, String name, String shapeId, ShapePointDTO[] shapes, PatternStopDTO[] patternStops, int useFrequency) throws InvalidNamespaceException, SQLException, IOException {
        // Create new route
        createSimpleTestRoute(routeId, "RTA", "500", "Hollingsworth", 3);
        // Create new pattern for route
        PatternDTO input = new PatternDTO();
        input.pattern_id = patternId;
        input.route_id = routeId;
        input.name = name;
        input.use_frequency = useFrequency;
        input.shape_id = shapeId;
        input.shapes = shapes;
        input.pattern_stops = patternStops;
        // Write the pattern to the database
        JdbcTableWriter createPatternWriter = createTestTableWriter(Table.PATTERNS);
        String output = createPatternWriter.create(mapper.writeValueAsString(input), true);
        LOG.info("create {} output:", Table.PATTERNS.name);
        LOG.info(output);
        // Parse output
        return mapper.readValue(output, PatternDTO.class);
    }

    /**
     * Creates a pattern by first creating a route and then a pattern for that route.
     */
    private static PatternDTO createSimplePattern(String routeId, String patternId, String name, String shapeId)
        throws InvalidNamespaceException, SQLException, IOException {
        // Create new pattern for route
        PatternDTO input = new PatternDTO();
        input.pattern_id = patternId;
        input.route_id = routeId;
        input.name = name;
        input.use_frequency = 1;
        input.shape_id = shapeId;
        input.shapes = new ShapePointDTO[]{
            new ShapePointDTO(2, 0.0, shapeId, firstStopLat, firstStopLon, 0),
            new ShapePointDTO(2, 150.0, shapeId, lastStopLat, lastStopLon, 1)
        };
        input.pattern_stops = new PatternStopDTO[]{
            new PatternStopDTO(patternId, firstStopId, 0),
            new PatternStopDTO(patternId, lastStopId, 1)
        };
        // Write the pattern to the database
        JdbcTableWriter createPatternWriter = createTestTableWriter(Table.PATTERNS);
        String output = createPatternWriter.create(mapper.writeValueAsString(input), true);
        LOG.info("create {} output:", Table.PATTERNS.name);
        LOG.info(output);
        // Parse output
        return mapper.readValue(output, PatternDTO.class);
    }

    /**
     * Creates a pattern by first creating a route and then a pattern for that route.
     */
    private static PatternDTO createRouteAndSimplePattern(String routeId, String patternId, String name) throws InvalidNamespaceException, SQLException, IOException {
        return createRouteAndPattern(routeId, patternId, name, null, new ShapePointDTO[]{}, new PatternStopDTO[]{}, 0);
    }

    /**
     * Create and store a simple stop entity.
     */
    private static StopDTO createSimpleStop(String stopId, String stopName, double latitude, double longitude) throws InvalidNamespaceException, IOException, SQLException {
        JdbcTableWriter createStopWriter = new JdbcTableWriter(Table.STOPS, testDataSource, testNamespace);
        StopDTO input = new StopDTO();
        input.stop_id = stopId;
        input.stop_name = stopName;
        input.stop_lat = latitude;
        input.stop_lon = longitude;
        String output = createStopWriter.create(mapper.writeValueAsString(input), true);
        LOG.info("create {} output:", Table.STOPS.name);
        LOG.info(output);
        return mapper.readValue(output, StopDTO.class);
    }

    /**
     * Create and store a simple route for testing.
     */
    private static RouteDTO createSimpleTestRoute(String routeId, String agencyId, String shortName, String longName, int routeType) throws InvalidNamespaceException, IOException, SQLException {
        RouteDTO input = new RouteDTO();
        input.route_id = routeId;
        input.agency_id = agencyId;
        // Empty value should be permitted for transfers and transfer_duration
        input.route_short_name = shortName;
        input.route_long_name = longName;
        input.route_type = routeType;
        // convert object to json and save it
        JdbcTableWriter createTableWriter = createTestTableWriter(Table.ROUTES);
        String output = createTableWriter.create(mapper.writeValueAsString(input), true);
        LOG.info("create {} output:", Table.ROUTES.name);
        LOG.info(output);
        // parse output
        return mapper.readValue(output, RouteDTO.class);
    }

    /**
     * Create and store a simple calendar that runs on each weekday.
     */
    private static CalendarDTO createWeekdayCalendar(String serviceId, String startDate, String endDate) throws IOException, SQLException, InvalidNamespaceException {
        JdbcTableWriter createCalendarWriter = new JdbcTableWriter(Table.CALENDAR, testDataSource, testNamespace);
        CalendarDTO calendarInput = new CalendarDTO();
        calendarInput.service_id = serviceId;
        calendarInput.monday = 1;
        calendarInput.tuesday = 1;
        calendarInput.wednesday = 1;
        calendarInput.thursday = 1;
        calendarInput.friday = 1;
        calendarInput.saturday = 0;
        calendarInput.sunday = 0;
        calendarInput.start_date = startDate;
        calendarInput.end_date = endDate;
        String output = createCalendarWriter.create(mapper.writeValueAsString(calendarInput), true);
        LOG.info("create {} output:", Table.CALENDAR.name);
        LOG.info(output);
        return mapper.readValue(output, CalendarDTO.class);
    }

    /**
     * Create and store a simple calendar date that modifies service on one day.
     */
    private static CalendarDateDTO createAndStoreCalendarDate(String serviceId, String date, int exceptionType) throws IOException, SQLException, InvalidNamespaceException {
        JdbcTableWriter createCalendarDateWriter = new JdbcTableWriter(Table.CALENDAR_DATES, testDataSource, testNamespace);

        CalendarDateDTO calendarDate = createCalendarDate(serviceId, date, exceptionType);
        String output = createCalendarDateWriter.create(mapper.writeValueAsString(calendarDate), true);

        LOG.info("create {} output:", Table.CALENDAR_DATES.name);
        LOG.info(output);

        return mapper.readValue(output, CalendarDateDTO.class);
    }

    /**
     * Create a calendar date.
     */
    private static CalendarDateDTO createCalendarDate(String serviceId, String date, Integer exceptionType) {
        CalendarDateDTO calenderDate = new CalendarDateDTO();
        calenderDate.date = date;
        calenderDate.service_id = serviceId;
        calenderDate.exception_type = exceptionType;
        return calenderDate;
    }
}
