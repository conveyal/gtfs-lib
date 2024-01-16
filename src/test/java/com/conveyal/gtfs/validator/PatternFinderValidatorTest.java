package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.TestUtils;
import com.conveyal.gtfs.loader.FeedLoadResult;
import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.model.Pattern;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.conveyal.gtfs.GTFS.load;
import static com.conveyal.gtfs.GTFS.validate;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;


class PatternFinderValidatorTest {

    private static final Logger LOG = LoggerFactory.getLogger(PatternFinderValidatorTest.class);

    private static String testDBName;

    private static DataSource testDataSource;

    @BeforeAll
    public static void setUpClass() throws Exception {
        // create a new database
        testDBName = TestUtils.generateNewDB();
        String dbConnectionUrl = String.format("jdbc:postgresql://localhost/%s", testDBName);
        testDataSource = TestUtils.createTestDataSource(dbConnectionUrl);
    }

    @AfterAll
    public static void tearDownClass() {
        TestUtils.dropDB(testDBName);
    }

    @Test
    void canUseFeedPatterns() throws SQLException, IOException {
        String zipFileName = TestUtils.zipFolderFiles("real-world-gtfs-feeds/RABA", true);
        FeedLoadResult feedLoadResult = load(zipFileName, testDataSource);
        String testNamespace = feedLoadResult.uniqueIdentifier;
        validate(testNamespace, testDataSource);
        checkPatternStops(zipFileName, testNamespace);
    }

    @Test
    void canRevertToGeneratedPatterns() throws SQLException, IOException {
        String zipFileName = TestUtils.zipFolderFiles("fake-agency", true);
        FeedLoadResult feedLoadResult = load(zipFileName, testDataSource);
        String testNamespace = feedLoadResult.uniqueIdentifier;
        validate(testNamespace, testDataSource);
        checkPatternStops(zipFileName, testNamespace);
    }

    private void checkPatternStops(String zipFileName, String testNamespace) throws SQLException {
        GTFSFeed feed = GTFSFeed.fromFile(zipFileName);
        for (String key : feed.patterns.keySet()) {
            Pattern pattern = feed.patterns.get(key);
            assertThatSqlQueryYieldsRowCountGreaterThanZero(
                String.format(
                    "select * from %s where pattern_id = '%s'",
                    String.format("%s.%s", testNamespace, Table.PATTERN_STOP.name),
                    pattern.pattern_id
                )
            );
        }
    }

    private void assertThatSqlQueryYieldsRowCountGreaterThanZero(String sql) throws SQLException {
        int recordCount = 0;
        ResultSet rs = testDataSource.getConnection().prepareStatement(sql).executeQuery();
        while (rs.next()) recordCount++;
        assertThat(recordCount, greaterThan(0));
    }
}