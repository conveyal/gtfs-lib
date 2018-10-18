package com.conveyal.gtfs.graphql.fetchers;

import com.conveyal.gtfs.graphql.GTFSGraphQL;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.conveyal.gtfs.graphql.GraphQLUtil.namespacedTableFieldName;
import static com.conveyal.gtfs.graphql.GraphQLUtil.namespacedTableName;
import static com.conveyal.gtfs.graphql.fetchers.JDBCFetcher.validateNamespace;

/**
 * This wraps an SQL row fetcher, extracting only a single column of the specified type.
 * Because there's only one column, it collapses the result down into a list of elements of that column's type,
 * rather than a list of maps (one for each row) as the basic SQL fetcher does.
 */
public class SQLColumnFetcher<T> implements DataFetcher<List<T>> {

    public static final Logger LOG = LoggerFactory.getLogger(SQLColumnFetcher.class);

    public final String columnName;

    private final JDBCFetcher jdbcFetcher;

    /**
     * Constructor for tables that don't need any restriction by a where clause based on the enclosing entity.
     * These would typically be at the topmost level, directly inside a feed rather than nested in some GTFS entity type.
     */
    public SQLColumnFetcher(String tableName, String parentJoinField, String columnName) {
        this.columnName = columnName;
        this.jdbcFetcher = new JDBCFetcher(tableName, parentJoinField);
    }

    /**
     * This get method does not use the JdbcFetcher get method because the query ultimately returns a list of values
     * instead of a List of a Map of Objects.
     */
    @Override
    public List<T> get (DataFetchingEnvironment environment) {
        // GetSource is the context in which this this DataFetcher has been created, in this case a map representing
        // the parent feed (FeedFetcher).
        Map<String, Object> parentEntityMap = environment.getSource();
        String namespace = (String) parentEntityMap.get("namespace");
        validateNamespace(namespace);
        DataSource dataSource = GTFSGraphQL.getDataSourceFromContext(environment);

        // get id to filter by
        String filterValue = (String) parentEntityMap.get(jdbcFetcher.parentJoinField);
        if (filterValue == null) {
            return new ArrayList<>();
        }

        StringBuilder sqlStatementBuilder = new StringBuilder();
        sqlStatementBuilder.append(String.format(
            "select %s from %s where %s = ?",
            namespacedTableFieldName(namespace, jdbcFetcher.tableName, columnName),
            namespacedTableName(namespace, jdbcFetcher.tableName),
            namespacedTableFieldName(namespace, jdbcFetcher.tableName, jdbcFetcher.parentJoinField)
        ));

        List<T> results = new ArrayList<>();
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            for (Map<String, Object> row : jdbcFetcher.getSqlQueryResult(
                connection,
                namespace,
                sqlStatementBuilder.toString(),
                Arrays.asList(filterValue)
                )
            ) {
                results.add((T) row.get(columnName));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DbUtils.closeQuietly(connection);
        }
        return results;
    }

}
