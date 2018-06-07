package com.conveyal.gtfs.graphql;

import com.conveyal.gtfs.TestUtils;
import com.conveyal.gtfs.loader.FeedLoadResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.GraphQL;
import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static com.conveyal.gtfs.GTFS.createDataSource;
import static com.conveyal.gtfs.GTFS.load;
import static com.conveyal.gtfs.GTFS.validate;
import static com.conveyal.gtfs.TestUtils.getResourceFileName;
import static com.zenika.snapshotmatcher.SnapshotMatcher.matchesSnapshot;
import static org.hamcrest.MatcherAssert.assertThat;

public class GTFSGraphQLTest {
    private static String testDBName;
    private static DataSource testDataSource;
    private static String testNamespace;

    private ObjectMapper mapper = new ObjectMapper();

    @BeforeClass
    public static void setUpClass() throws SQLException, IOException {
        // create a new database
        testDBName = TestUtils.generateNewDB();
        String dbConnectionUrl = "jdbc:postgresql://localhost/" + testDBName;
        testDataSource = createDataSource(dbConnectionUrl, null, null);
        // zip up test folder into temp zip file
        String zipFileName = TestUtils.zipFolderFiles("fake-agency");
        // load feed into db
        FeedLoadResult feedLoadResult = load(zipFileName, testDataSource);
        testNamespace = feedLoadResult.uniqueIdentifier;
        // validate feed to create additional tables
        validate(testNamespace, testDataSource);
    }

    @AfterClass
    public static void tearDownClass() {
        TestUtils.dropDB(testDBName);
    }

    // tests that the graphQL schema can initialize
    @Test
    public void canInitialize() {
        GTFSGraphQL.initialize(testDataSource);
        GraphQL graphQL = GTFSGraphQL.getGraphQl();
    }

    // tests that the root element of a feed can be fetched
    @Test
    public void canFetchFeed() throws IOException {
        assertThat(queryGraphQL("feed.txt"), matchesSnapshot());
    }

    // tests that the row counts of a feed can be fetched
    @Test
    public void canFetchFeedRowCounts() throws IOException {
        assertThat(queryGraphQL("feedRowCounts.txt"), matchesSnapshot());
    }

    // tests that the errors of a feed can be fetched
    @Test
    public void canFetchErrors() throws IOException {
        assertThat(queryGraphQL("feedErrors.txt"), matchesSnapshot());
    }

    // tests that the feed_info of a feed can be fetched
    @Test
    public void canFetchFeedInfo() throws IOException {
        assertThat(queryGraphQL("feedFeedInfo.txt"), matchesSnapshot());
    }

    // tests that the feed_info of a feed can be fetched
    @Test
    public void canFetchPatterns() throws IOException {
        assertThat(queryGraphQL("feedPatterns.txt"), matchesSnapshot());
    }

    /**
     * Helper method to make a query with default variables
     */
    private Map<String, Object> queryGraphQL(String queryFilename) throws IOException {
        Map<String, Object> variables = new HashMap<String, Object>();
        variables.put("namespace", testNamespace);
        return queryGraphQL(queryFilename, variables);
    }

    /**
     * Helper method to execute a GraphQL query and return the result
     */
    private Map<String, Object> queryGraphQL(String queryFilename, Map<String,Object> variables) throws IOException {
        GTFSGraphQL.initialize(testDataSource);
        FileInputStream inputStream = new FileInputStream(getResourceFileName("graphql/" + queryFilename));
        return GTFSGraphQL.getGraphQl().execute(
            IOUtils.toString(inputStream),
            null,
            null,
            variables
        ).toSpecification();
    }
}
