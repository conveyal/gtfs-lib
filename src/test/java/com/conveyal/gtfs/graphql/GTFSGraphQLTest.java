package com.conveyal.gtfs.graphql;

import com.conveyal.gtfs.TestUtils;
import graphql.GraphQL;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.SQLException;

import static com.conveyal.gtfs.GTFS.createDataSource;

public class GTFSGraphQLTest {
    private static String testDBName;
    private static DataSource testDataSource;

    @BeforeClass
    public static void setUpClass() throws SQLException {
        // create a new database
        testDBName = TestUtils.generateNewDB();
        String dbConnectionUrl = "jdbc:postgresql://localhost/" + testDBName;
        testDataSource = createDataSource(dbConnectionUrl, null, null);
    }

    @AfterClass
    public static void tearDownClass() {
        TestUtils.dropDB(testDBName);
    }
    
    @Test
    public void canInitialize() {
        GTFSGraphQL.initialize(testDataSource);
        GraphQL graphQL = GTFSGraphQL.getGraphQl();
    }
}
