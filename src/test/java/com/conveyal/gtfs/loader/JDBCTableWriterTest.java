package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.TestUtils;
import com.conveyal.gtfs.util.FareDTO;
import com.conveyal.gtfs.util.FeedInfoDTO;
import com.conveyal.gtfs.util.InvalidNamespaceException;
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

import static com.conveyal.gtfs.GTFS.createDataSource;
import static com.conveyal.gtfs.GTFS.makeSnapshot;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

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
        LOG.info("create output:");
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
        LOG.info("update output:");
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
        LOG.info("delete output:");
        LOG.info(updateOutput);

        // make sure record does not exist in DB
        String sql = String.format(
            "select * from %s.%s where id=%d",
            testNamespace,
            feedInfoTable.name,
            createdFeedInfo.id
        );
        LOG.info(sql);
        ResultSet rs = testDataSource.getConnection().prepareStatement(sql).executeQuery();
        assertThat(rs.getFetchSize(), equalTo(0));
    }

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
        fareInput.transfers = null;
        fareInput.fare_rules = new FareDTO.FareRuleDTO[]{};

        // convert object to json and save it
        JdbcTableWriter createTableWriter = createTestTableWriter(fareTable);
        String createOutput = createTableWriter.create(mapper.writeValueAsString(fareInput), true);
        LOG.info("create output:");
        LOG.info(createOutput);

        // parse output
        FareDTO createdFare = mapper.readValue(createOutput, fareDTOClass);

        // make sure saved data matches expected data
        assertThat(createdFare.fare_id, equalTo(fareId));

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
        LOG.info("update output:");
        LOG.info(updateOutput);

        FareDTO updatedFareDTO = mapper.readValue(updateOutput, fareDTOClass);

        // make sure saved data matches expected data
        assertThat(updatedFareDTO.fare_id, equalTo(updatedFareId));

        // try to delete record
        JdbcTableWriter deleteTableWriter = createTestTableWriter(fareTable);
        int deleteOutput = deleteTableWriter.delete(
                createdFare.id,
                true
        );
        LOG.info("delete output:");
        LOG.info(updateOutput);

        // make sure record does not exist in DB
        String sql = String.format(
                "select * from %s.%s where id=%d",
                testNamespace,
                fareTable.name,
                createdFare.id
        );
        LOG.info(sql);
        ResultSet rs = testDataSource.getConnection().prepareStatement(sql).executeQuery();
        assertThat(rs.getFetchSize(), equalTo(0));
    }

    @AfterClass
    public static void tearDownClass() {
        TestUtils.dropDB(testDBName);
    }
}
