package com.conveyal.gtfs.graphql.fetchers;

import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.graphql.GTFSGraphQL;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Get quantity of errors broken down by error type.
 * GraphQL does not have a type for arbitrary maps (String -> X). Such maps must be expressed as a list of
 * key-value pairs. This is probably intended to protect us from ourselves (sending untyped data) but it leads to
 * silly workarounds like this where there are a large number of possible keys.
 *
 * "Unfortunately what you'd like to do is not possible. GraphQL requires you to be explicit about specifying which
 * fields you would like returned from your query."
 * "Ok, and if I request some object of an unknown form from backend which I'm supposed to proxy or send back?"
 * "the whole idea of graphql is that there is no such thing as an 'unkown form'".
 * https://stackoverflow.com/a/34226484/778449
 */
public class ErrorCountFetcher implements DataFetcher {

    public static final Logger LOG = LoggerFactory.getLogger(ErrorCountFetcher.class);

    @Override
    public Object get(DataFetchingEnvironment environment) {
        List<ErrorCount> errorCounts = new ArrayList();
        Map<String, Object> parentFeedMap = environment.getSource();
        String namespace = (String) parentFeedMap.get("namespace");
        DataSource dataSource = GTFSGraphQL.getDataSourceFromContext(environment);
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
            String sql = String.format("select error_type, count(*) from %s.errors group by error_type", namespace);
            LOG.info("SQL: {}", sql);
            if (statement.execute(sql)) {
                ResultSet resultSet = statement.getResultSet();
                while (resultSet.next()) {
                    errorCounts.add(new ErrorCount(NewGTFSErrorType.valueOf(resultSet.getString(1)), resultSet.getInt(2)));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DbUtils.closeQuietly(connection);
        }
        return errorCounts;
    }

    public static class ErrorCount {
        public NewGTFSErrorType type;
        public int count;
        public String message;

        public ErrorCount(NewGTFSErrorType errorType, int count) {
            this.type = errorType;
            this.count = count;
            this.message = errorType.englishMessage;
        }
    }

}
