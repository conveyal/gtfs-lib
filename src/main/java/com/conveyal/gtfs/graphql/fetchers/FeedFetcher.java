package com.conveyal.gtfs.graphql.fetchers;

import com.conveyal.gtfs.graphql.GTFSGraphQL;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static com.conveyal.gtfs.graphql.fetchers.JDBCFetcher.validateNamespace;

/**
 * Fetch the summary row for a particular loaded feed, based on its namespace.
 * This essentially gets the row from the top-level summary table of all feeds that have been loaded into the database.
 */
public class FeedFetcher implements DataFetcher {

    public static final Logger LOG = LoggerFactory.getLogger(DataFetcher.class);

    @Override
    public Map<String, Object> get (DataFetchingEnvironment environment) {
        String namespace = environment.getArgument("namespace"); // This is the unique table prefix (the "schema").
        validateNamespace(namespace);
        Connection connection = null;
        try {
            connection = GTFSGraphQL.getConnection();
            PreparedStatement statement = connection.prepareStatement("select * from feeds where namespace = ?");
            statement.setString(1, namespace);
            LOG.debug("SQL: {}", statement.toString());
            if (statement.execute()) {
                ResultSet resultSet = statement.getResultSet();
                ResultSetMetaData meta = resultSet.getMetaData();
                int nColumns = meta.getColumnCount();
                // Iterate over result rows
                while (resultSet.next()) {
                    // Create a Map to hold the contents of this row, injecting the feed_id into every map
                    Map<String, Object> resultMap = new HashMap<>();
                    resultMap.put("namespace", namespace);
                    for (int i = 1; i <= nColumns; i++) {
                        resultMap.put(meta.getColumnName(i), resultSet.getObject(i));
                    }
                    connection.close();
                    // FIXME return inside a while loop? This would only hit the first item.
                    return resultMap;
                }
            }
            throw new RuntimeException("No rows found.");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DbUtils.closeQuietly(connection);
        }
    }

}
