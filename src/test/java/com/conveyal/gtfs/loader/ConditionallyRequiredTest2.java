package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.TestUtils;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.IOException;

import static com.conveyal.gtfs.GTFS.load;
import static com.conveyal.gtfs.GTFS.validate;
import static com.conveyal.gtfs.TestUtils.assertThatSqlCountQueryYieldsExpectedCount;
import static com.conveyal.gtfs.error.NewGTFSErrorType.REFERENTIAL_INTEGRITY;

/**
 * Similar to ConditionallyRequiredTest
 * except that it uses a different feed and checks a referential integrity error.
 */
// TODO: Refactor this class or extract common parts.
public class ConditionallyRequiredTest2 {
    private static String testDBName;
    private static DataSource testDataSource;
    private static String testNamespace;

    @BeforeAll
    public static void setUpClass() throws IOException {
        // Create a new database
        testDBName = TestUtils.generateNewDB();
        String dbConnectionUrl = String.format("jdbc:postgresql://localhost/%s", testDBName);
        testDataSource = TestUtils.createTestDataSource(dbConnectionUrl);
        // load feed into db
        String zipFileName = TestUtils.zipFolderFiles(
            "real-world-gtfs-feeds/tri-delta-fare-rules",
            true);
        FeedLoadResult feedLoadResult = load(zipFileName, testDataSource);
        testNamespace = feedLoadResult.uniqueIdentifier;
        validate(testNamespace, testDataSource);
    }

    @AfterAll
    public static void tearDownClass() {
        TestUtils.dropDB(testDBName);
    }

    @Test
    void shouldNotTriggerRefIntegrityError() {
        checkFeedHasZeroError(
            REFERENTIAL_INTEGRITY,
            "FareRule",
            "2",
            "1",
            null
        );
    }

    /**
     * Check that the test feed has exactly one error for the provided values.
     */
    private void checkFeedHasZeroError(NewGTFSErrorType errorType, String entityType, String lineNumber, String entityId, String badValue) {
        String sql = String.format("select count(*) from %s.errors where error_type = '%s' and entity_type = '%s' and line_number = '%s'",
            testNamespace,
            errorType,
            entityType,
            lineNumber);

        if (entityId != null) sql += String.format(" and entity_id = '%s'", entityId);
        if (badValue != null) sql += String.format(" and bad_value = '%s'", badValue);

        assertThatSqlCountQueryYieldsExpectedCount(testDataSource, sql,0);
    }
}
