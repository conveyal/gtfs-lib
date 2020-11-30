package com.conveyal.gtfs.graphql;

import com.conveyal.gtfs.TestUtils;
import com.conveyal.gtfs.loader.FeedLoadResult;
import graphql.ExecutionInput;
import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.conveyal.gtfs.GTFS.createDataSource;
import static com.conveyal.gtfs.GTFS.load;
import static com.conveyal.gtfs.GTFS.validate;
import static com.conveyal.gtfs.TestUtils.getResourceFileName;
import static com.zenika.snapshotmatcher.SnapshotMatcher.matchesSnapshot;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * A test suite for all things related to fetching objects with GraphQL.
 * Important note: Snapshot testing is heavily used in this test suite and can potentially result in false negatives
 *   in cases where the order of items in a list is not important.
 */
public class GTFSGraphQLTest {
    private static String testDBName;
    private static DataSource testDataSource;
    private static String testNamespace;

    private static String testInjectionDBName;
    private static DataSource testInjectionDataSource;
    private static String testInjectionNamespace;
    private static String badCalendarDateNamespace;

    @BeforeClass
    public static void setUpClass() throws IOException {
        // create a new database
        testDBName = TestUtils.generateNewDB();
        String dbConnectionUrl = String.format("jdbc:postgresql://localhost/%s", testDBName);
        testDataSource = createDataSource(dbConnectionUrl, null, null);
        // zip up test folder into temp zip file
        String zipFileName = TestUtils.zipFolderFiles("fake-agency", true);
        // load feed into db
        FeedLoadResult feedLoadResult = load(zipFileName, testDataSource);
        testNamespace = feedLoadResult.uniqueIdentifier;
        // validate feed to create additional tables
        validate(testNamespace, testDataSource);

        // Zip up test folder into temp zip file for loading into database.
        FeedLoadResult feed2LoadResult = load(
            TestUtils.zipFolderFiles("fake-agency-bad-calendar-date", true),
            testDataSource
        );
        badCalendarDateNamespace = feed2LoadResult.uniqueIdentifier;
        // validate feed to create additional tables
        validate(badCalendarDateNamespace, testDataSource);

        // create a separate injection database to use in injection tests
        // create a new database
        testInjectionDBName = TestUtils.generateNewDB();
        String injectionDbConnectionUrl = String.format("jdbc:postgresql://localhost/%s", testInjectionDBName);
        testInjectionDataSource = createDataSource(injectionDbConnectionUrl, null, null);
        // load feed into db
        FeedLoadResult injectionFeedLoadResult = load(zipFileName, testInjectionDataSource);
        testInjectionNamespace = injectionFeedLoadResult.uniqueIdentifier;
        // validate feed to create additional tables
        validate(testInjectionNamespace, testInjectionDataSource);
    }

    @AfterClass
    public static void tearDownClass() {
        TestUtils.dropDB(testDBName);
        TestUtils.dropDB(testInjectionDBName);
    }

    /** Tests that the graphQL schema can initialize. */
    @Test(timeout=5000)
    public void canInitialize() {
        GTFSGraphQL.initialize(testDataSource);
        GTFSGraphQL.getGraphQl();
    }

    /** Tests that the root element of a feed can be fetched. */
    @Test(timeout=5000)
    public void canFetchFeed() throws IOException {
        assertThat(queryGraphQL("feed.txt"), matchesSnapshot());
    }

    /** Tests that the row counts of a feed can be fetched. */
    @Test(timeout=5000)
    public void canFetchFeedRowCounts() throws IOException {
        assertThat(queryGraphQL("feedRowCounts.txt"), matchesSnapshot());
    }

    /** Tests that the errors of a feed can be fetched. */
    @Test(timeout=5000)
    public void canFetchErrors() throws IOException {
        assertThat(queryGraphQL("feedErrors.txt"), matchesSnapshot());
    }

    /** Tests that the feed_info of a feed can be fetched. */
    @Test(timeout=5000)
    public void canFetchFeedInfo() throws IOException {
        assertThat(queryGraphQL("feedFeedInfo.txt"), matchesSnapshot());
    }

    /** Tests that the patterns of a feed can be fetched. */
    @Test(timeout=5000)
    public void canFetchPatterns() throws IOException {
        assertThat(queryGraphQL("feedPatterns.txt"), matchesSnapshot());
    }

    /** Tests that the patterns of a feed can be fetched. */
    @Test(timeout=5000)
    public void canFetchPolylines() throws IOException {
        assertThat(queryGraphQL("feedPolylines.txt"), matchesSnapshot());
    }

    /** Tests that the agencies of a feed can be fetched. */
    @Test(timeout=5000)
    public void canFetchAgencies() throws IOException {
        assertThat(queryGraphQL("feedAgencies.txt"), matchesSnapshot());
    }

    /** Tests that the calendars of a feed can be fetched. */
    @Test(timeout=5000)
    public void canFetchCalendars() throws IOException {
        assertThat(queryGraphQL("feedCalendars.txt"), matchesSnapshot());
    }

    /** Tests that the fares of a feed can be fetched. */
    @Test(timeout=5000)
    public void canFetchFares() throws IOException {
        assertThat(queryGraphQL("feedFares.txt"), matchesSnapshot());
    }

    /** Tests that the routes of a feed can be fetched. */
    @Test(timeout=5000)
    public void canFetchRoutes() throws IOException {
        assertThat(queryGraphQL("feedRoutes.txt"), matchesSnapshot());
    }

    /** Tests that the stops of a feed can be fetched. */
    @Test(timeout=5000)
    public void canFetchStops() throws IOException {
        assertThat(queryGraphQL("feedStops.txt"), matchesSnapshot());
    }

    /** Tests that the stops of a feed can be fetched. */
    @Test(timeout=5000)
    public void canFetchStopWithChildren() throws IOException {
        assertThat(queryGraphQL("feedStopWithChildren.txt"), matchesSnapshot());
    }

    /** Tests that the trips of a feed can be fetched. */
    @Test(timeout=5000)
    public void canFetchTrips() throws IOException {
        assertThat(queryGraphQL("feedTrips.txt"), matchesSnapshot());
    }

    // TODO: make tests for schedule_exceptions / calendar_dates

    /** Tests that the stop times of a feed can be fetched. */
    @Test(timeout=5000)
    public void canFetchStopTimes() throws IOException {
        assertThat(queryGraphQL("feedStopTimes.txt"), matchesSnapshot());
    }

    /** Tests that the stop times of a feed can be fetched. */
    @Test(timeout=5000)
    public void canFetchServices() throws IOException {
        assertThat(queryGraphQL("feedServices.txt"), matchesSnapshot());
    }

    /** Tests that the stop times of a feed can be fetched. */
    @Test(timeout=5000)
    public void canFetchRoutesAndFilterTripsByDateAndTime() throws IOException {
        Map<String, Object> variables = new HashMap<>();
        variables.put("namespace", testNamespace);
        variables.put("date", "20170915");
        variables.put("from", 24000);
        variables.put("to", 28000);
        assertThat(
            queryGraphQL("feedRoutesAndTripsByTime.txt", variables, testDataSource),
            matchesSnapshot()
        );
    }

    /** Tests that the limit argument applies properly to a fetcher defined with autolimit set to false. */
    @Test(timeout=5000)
    public void canFetchNestedEntityWithLimit() throws IOException {
        assertThat(queryGraphQL("feedStopsStopTimeLimit.txt"), matchesSnapshot());
    }

    /** Tests whether a graphQL query that has superflous and redundant nesting can find the right result. */
    // if the graphQL dataloader is enabled correctly, there will not be any repeating sql queries in the logs
    @Test(timeout=5000)
    public void canFetchMultiNestedEntities() throws IOException {
        assertThat(queryGraphQL("superNested.txt"), matchesSnapshot());
    }

    /**
     * Tests whether a graphQL query that has superflous and redundant nesting can find the right result.
     * If the graphQL dataloader is enabled correctly, there will not be any repeating sql queries in the logs.
     * Furthermore, some queries should have been combined together.
     */
    @Test(timeout=5000)
    public void canFetchMultiNestedEntitiesWithoutLimits() throws IOException {
        assertThat(queryGraphQL("superNestedNoLimits.txt"), matchesSnapshot());
    }

    /** Tests that a query for child stops does not throw an exception for a feed with no
     * parent_station column in the imported stops table.
     */
    @Test(timeout=5000)
    public void canFetchStopsWithoutParentStationColumn() throws IOException {
        Map<String, Object> variables = new HashMap<>();
        variables.put("namespace", badCalendarDateNamespace);
        assertThat(
            queryGraphQL(
                "feedStopWithChildren.txt",
                variables,
                testDataSource
            ),
            matchesSnapshot()
        );
    }

    /**
     * Attempt to fetch more than one record with SQL injection as inputs.
     * The graphql library should properly escape the string and return 0 results for stops.
     */
    @Test(timeout=5000)
    public void canSanitizeSQLInjectionSentAsInput() throws IOException {
        Map<String, Object> variables = new HashMap<>();
        variables.put("namespace", testInjectionNamespace);
        variables.put("stop_id", Arrays.asList("' OR 1=1;"));
        assertThat(
            queryGraphQL(
                "feedStopsByStopId.txt",
                variables,
                testInjectionDataSource
            ),
            matchesSnapshot()
        );
    }

    /**
     * Attempt to run a graphql query when one of the pieces of data contains a SQL injection.
     * The graphql library should properly escape the string and complete the queries.
     */
    @Test(timeout=5000)
    public void canSanitizeSQLInjectionSentAsKeyValue() throws IOException, SQLException {
        // manually update the route_id key in routes and patterns
        String injection = "'' OR 1=1; Select ''99";
        Connection connection = testInjectionDataSource.getConnection();
        List<String> tablesToUpdate = Arrays.asList("routes", "fare_rules", "patterns", "trips");
        for (String table : tablesToUpdate) {
            connection.createStatement()
                .execute(String.format("update %s.%s set route_id = '%s'", testInjectionNamespace, table, injection));
        }
        connection.commit();

        // make graphql query
        Map<String, Object> variables = new HashMap<>();
        variables.put("namespace", testInjectionNamespace);
        assertThat(queryGraphQL("feedRoutes.txt", variables, testInjectionDataSource), matchesSnapshot());
    }


    /**
     * Helper method to make a query with default variables.
     *
     * @param queryFilename the filename that should be used to generate the GraphQL query.  This file must be present
     *                      in the `src/test/resources/graphql` folder
     */
    private Map<String, Object> queryGraphQL(String queryFilename) throws IOException {
        Map<String, Object> variables = new HashMap<>();
        variables.put("namespace", testNamespace);
        return queryGraphQL(queryFilename, variables, testDataSource);
    }

    /**
     * Helper method to execute a GraphQL query and return the result.
     *
     * @param queryFilename the filename that should be used to generate the GraphQL query.  This file must be present
     *                      in the `src/test/resources/graphql` folder
     * @param variables a Map of input variables to the graphql query about to be executed
     * @param dataSource the datasource to use when initializing GraphQL
     */
    private Map<String, Object> queryGraphQL(
        String queryFilename,
        Map<String,Object> variables,
        DataSource dataSource
    ) throws IOException {
        GTFSGraphQL.initialize(dataSource);
        FileInputStream inputStream = new FileInputStream(
            getResourceFileName(String.format("graphql/%s", queryFilename))
        );
        ExecutionInput executionInput = ExecutionInput.newExecutionInput()
            .query(IOUtils.toString(inputStream))
            .variables(variables)
            .build();
        return GTFSGraphQL.getGraphQl().execute(executionInput).toSpecification();
    }
}
