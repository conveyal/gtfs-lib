package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.TestUtils;
import com.conveyal.gtfs.util.CalendarDTO;
import com.conveyal.gtfs.util.FareDTO;
import com.conveyal.gtfs.util.FareRuleDTO;
import com.conveyal.gtfs.util.FeedInfoDTO;
import com.conveyal.gtfs.util.FrequencyDTO;
import com.conveyal.gtfs.util.InvalidNamespaceException;
import com.conveyal.gtfs.util.PatternDTO;
import com.conveyal.gtfs.util.PatternStopDTO;
import com.conveyal.gtfs.util.RouteDTO;
import com.conveyal.gtfs.util.ShapePointDTO;
import com.conveyal.gtfs.util.StopDTO;
import com.conveyal.gtfs.util.StopTimeDTO;
import com.conveyal.gtfs.util.TripDTO;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

import static com.conveyal.gtfs.GTFS.createDataSource;
import static com.conveyal.gtfs.GTFS.makeSnapshot;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

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
    private static final ObjectMapper mapper = new ObjectMapper();

    private static JdbcTableWriter createTestTableWriter (Table table) throws InvalidNamespaceException {
        return new JdbcTableWriter(table, testDataSource, testNamespace);
    }

    @BeforeClass
    public static void setUpClass() throws SQLException {
        // create a new database
        testDBName = TestUtils.generateNewDB();
        String dbConnectionUrl = String.format("jdbc:postgresql://localhost/%s", testDBName);
        testDataSource = createDataSource (dbConnectionUrl, null, null);
        LOG.info("creating feeds table because it isn't automatically generated unless you import a feed");
        Connection connection = testDataSource.getConnection();
        connection.createStatement()
            .execute("create table if not exists feeds (namespace varchar primary key, md5 varchar, " +
                "sha1 varchar, feed_id varchar, feed_version varchar, filename varchar, loaded_date timestamp, " +
                "snapshot_of varchar)");
        connection.commit();
        LOG.info("feeds table created");

        // create an empty snapshot to create a new namespace and all the tables
        FeedLoadResult result = makeSnapshot(null, testDataSource);
        testNamespace = result.uniqueIdentifier;
    }

    @Test
    public void canCreateUpdateAndDeleteFeedInfoEntities() throws IOException, SQLException, InvalidNamespaceException {
        // Store Table and Class values for use in test.
        final Table feedInfoTable = Table.FEED_INFO;
        final Class<FeedInfoDTO> feedInfoDTOClass = FeedInfoDTO.class;

        // create new object to be saved
        FeedInfoDTO feedInfoInput = new FeedInfoDTO();
        String publisherName = "test-publisher";
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
        assertThatSqlQueryYieldsZeroRows(String.format(
            "select * from %s.%s where id=%d",
            testNamespace,
            feedInfoTable.name,
            createdFeedInfo.id
        ));
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
        feedInfoInput.feed_publisher_name = publisherName;
        feedInfoInput.feed_publisher_url = "example.com";
        feedInfoInput.feed_lang = "en";
        feedInfoInput.default_route_color = "1c8edb";
        feedInfoInput.default_route_type = "3";

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
        // Empty value should be permitted for transfers and transfer_duration
        fareInput.transfers = null;
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

        // try to update record
        String updatedFareId = "3B";
        createdFare.fare_id = updatedFareId;

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

        // try to delete record
        JdbcTableWriter deleteTableWriter = createTestTableWriter(fareTable);
        int deleteOutput = deleteTableWriter.delete(
                createdFare.id,
                true
        );
        LOG.info("deleted {} records from {}", deleteOutput, fareTable.name);

        // make sure fare_attributes record does not exist in DB
        assertThatSqlQueryYieldsZeroRows(String.format(
                "select * from %s.%s where id=%d",
                testNamespace,
                fareTable.name,
                createdFare.id
        ));

        // make sure fare_rules record does not exist in DB
        assertThatSqlQueryYieldsZeroRows(String.format(
                "select * from %s.%s where id=%d",
                testNamespace,
                Table.FARE_RULES.name,
                createdFare.fare_rules[0].id
        ));
    }

    private void assertThatSqlQueryYieldsZeroRows(String sql) throws SQLException {
        assertThatSqlQueryYieldsRowCount(sql, 0);
    }

    private void assertThatSqlQueryYieldsRowCount(String sql, int expectedRowCount) throws SQLException {
        LOG.info(sql);
        ResultSet resultSet = testDataSource.getConnection().prepareStatement(sql).executeQuery();
        assertThat(resultSet.getFetchSize(), equalTo(expectedRowCount));
    }

    @Test
    public void canCreateUpdateAndDeleteRoutes() throws IOException, SQLException, InvalidNamespaceException {
        // Store Table and Class values for use in test.
        final Table routeTable = Table.ROUTES;
        final Class<RouteDTO> routeDTOClass = RouteDTO.class;

        // create new object to be saved
        String routeId = "500";
        RouteDTO createdRoute = createSimpleTestRoute(routeId, "RTA", "500", "Hollingsworth", 3);

        // make sure saved data matches expected data
        assertThat(createdRoute.route_id, equalTo(routeId));
        // TODO: Verify with a SQL query that the database now contains the created data (we may need to use the same
        //       db connection to do this successfully?)

        // try to update record
        String updatedRouteId = "600";
        createdRoute.route_id = updatedRouteId;

        // covert object to json and save it
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
        // TODO: Verify with a SQL query that the database now contains the updated data (we may need to use the same
        //       db connection to do this successfully?)

        // try to delete record
        JdbcTableWriter deleteTableWriter = createTestTableWriter(routeTable);
        int deleteOutput = deleteTableWriter.delete(
                createdRoute.id,
                true
        );
        LOG.info("deleted {} records from {}", deleteOutput, routeTable.name);

        // make sure route record does not exist in DB
        assertThatSqlQueryYieldsZeroRows(String.format(
                "select * from %s.%s where id=%d",
                testNamespace,
                routeTable.name,
                createdRoute.id
        ));
    }

    /**
     * Creates a simple route for testing.
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
     * Creates a pattern by first creating a route and then a pattern for that route.
     */
    private static PatternDTO createSimpleRouteAndPattern(String routeId, String patternId, String name) throws InvalidNamespaceException, SQLException, IOException {
        // Create new route
        createSimpleTestRoute(routeId, "RTA", "500", "Hollingsworth", 3);
        // Create new pattern for route
        PatternDTO input = new PatternDTO();
        input.pattern_id = patternId;
        input.route_id = routeId;
        input.name = name;
        input.use_frequency = 0;
        input.shapes = new ShapePointDTO[]{};
        input.pattern_stops = new PatternStopDTO[]{};
        // Write the pattern to the database
        JdbcTableWriter createPatternWriter = createTestTableWriter(Table.PATTERNS);
        String output = createPatternWriter.create(mapper.writeValueAsString(input), true);
        LOG.info("create {} output:", Table.PATTERNS.name);
        LOG.info(output);
        // Parse output
        return mapper.readValue(output, PatternDTO.class);
    }

    /**
     * Checks that creating, updating, and deleting a trip functions properly. As a part of this test, a service calendar,
     * stops, a route, and a pattern are all created because they are necessary for a trip to be stored. This also tests
     * updating a pattern with pattern stops and creating a trip with a frequency entry.
     */
    @Test
    public void canCreateUpdateAndDeleteTrips() throws IOException, SQLException, InvalidNamespaceException {
        // Store Table and Class values for use in test.
        final Table tripsTable = Table.TRIPS;
        // Create service calendar and stops, both of which are necessary to construct a complete pattern.
        String serviceId = "1";
        createWeekdayCalendar(serviceId, "20180103", "20180104");
        String firstStopId = "1";
        String lastStopId = "2";
        createSimpleStop(firstStopId, "First Stop", 34.2222, -87.333);
        createSimpleStop(lastStopId, "Last Stop", 34.2233, -87.334);
        String routeId = "700";
        String patternId = UUID.randomUUID().toString();
        PatternDTO createdPattern = createSimpleRouteAndPattern(routeId, patternId, "The Loop");
        // TODO Test that a frequency trip entry cannot be added for a timetable-based pattern.
        // Update pattern with pattern stops, set to use frequencies, and TODO shape points
        JdbcTableWriter patternUpdater = createTestTableWriter(Table.PATTERNS);
        createdPattern.use_frequency = 1;
        createdPattern.pattern_stops = new PatternStopDTO[]{
            new PatternStopDTO(patternId, firstStopId, 0),
            new PatternStopDTO(patternId, lastStopId, 1)
        };
        String updatedPatternOutput = patternUpdater.update(createdPattern.id, mapper.writeValueAsString(createdPattern), true);
        LOG.info("Updated pattern output: {}", updatedPatternOutput);
        // Create new trip for the pattern
        JdbcTableWriter createTripWriter = createTestTableWriter(tripsTable);
        TripDTO tripInput = new TripDTO();
        tripInput.pattern_id = createdPattern.pattern_id;
        tripInput.route_id = routeId;
        tripInput.service_id = serviceId;
        tripInput.stop_times = new StopTimeDTO[]{
            new StopTimeDTO(firstStopId, 0, 0, 0),
            new StopTimeDTO(lastStopId, 60, 60, 1)
        };
        FrequencyDTO frequency = new FrequencyDTO();
        int startTime = 6 * 60 * 60;
        frequency.start_time = startTime;
        frequency.end_time = 9 * 60 * 60;
        frequency.headway_secs = 15 * 60;
        tripInput.frequencies = new FrequencyDTO[]{frequency};
        String createTripOutput = createTripWriter.create(mapper.writeValueAsString(tripInput), true);
        TripDTO createdTrip = mapper.readValue(createTripOutput, TripDTO.class);
        // Update trip
        // TODO: Add update and delete tests for updating pattern stops, stop_times, and frequencies.
        String updatedTripId = "100A";
        createdTrip.trip_id = updatedTripId;
        JdbcTableWriter updateTripWriter = createTestTableWriter(tripsTable);
        String updateTripOutput = updateTripWriter.update(tripInput.id, mapper.writeValueAsString(createdTrip), true);
        TripDTO updatedTrip = mapper.readValue(updateTripOutput, TripDTO.class);
        // Check that saved data matches expected data
        assertThat(updatedTrip.frequencies[0].start_time, equalTo(startTime));
        assertThat(updatedTrip.trip_id, equalTo(updatedTripId));
        // Delete trip record
        JdbcTableWriter deleteTripWriter = createTestTableWriter(tripsTable);
        int deleteOutput = deleteTripWriter.delete(
            createdTrip.id,
            true
        );
        LOG.info("deleted {} records from {}", deleteOutput, tripsTable.name);
        // Check that route record does not exist in DB
        assertThatSqlQueryYieldsZeroRows(
            String.format(
                "select * from %s.%s where id=%d",
                testNamespace,
                tripsTable.name,
                createdTrip.id
            ));
    }

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

    @AfterClass
    public static void tearDownClass() {
        TestUtils.dropDB(testDBName);
    }
}
