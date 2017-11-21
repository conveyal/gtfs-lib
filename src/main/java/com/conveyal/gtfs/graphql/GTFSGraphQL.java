package com.conveyal.gtfs.graphql;

import com.conveyal.gtfs.GTFS;
import graphql.GraphQL;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * This provides a GraphQL API around the gtfs-lib JDBC storage.
 * This just makes a Java API with the right schema available, which uses String requests and responses.
 * To make this into a web API you need to wrap it in an HTTP framework / server.
 */
public class GTFSGraphQL {

    private static DataSource dataSource;

    // TODO Is it correct to share one of these objects between many instances? Is it supposed to be long-lived or threadsafe?
    // Analysis-backend creates a new GraphQL object on every request.
    private static GraphQL GRAPHQL;

    /** Username and password can be null if connecting to a local instance with host-based authentication. */
    public static void initialize (DataSource dataSource) {
        GTFSGraphQL.dataSource = dataSource;
        GRAPHQL = new GraphQL(GraphQLGtfsSchema.feedBasedSchema);
    }

    public static Connection getConnection() {
        try {
            return dataSource.getConnection();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static GraphQL getGraphQl () {
        return GRAPHQL;
    }

}
