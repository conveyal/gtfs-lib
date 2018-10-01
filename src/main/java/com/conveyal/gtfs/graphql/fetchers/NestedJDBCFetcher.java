package com.conveyal.gtfs.graphql.fetchers;

import com.conveyal.gtfs.graphql.GraphQLGtfsSchema;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.conveyal.gtfs.graphql.GTFSGraphQL.getDataSourceFromContext;
import static com.conveyal.gtfs.graphql.GTFSGraphQL.getJdbcQueryDataLoaderFromContext;
import static com.conveyal.gtfs.graphql.GraphQLUtil.multiStringArg;
import static com.conveyal.gtfs.graphql.GraphQLUtil.namespacedTableFieldName;
import static com.conveyal.gtfs.graphql.GraphQLUtil.namespacedTableName;
import static com.conveyal.gtfs.graphql.GraphQLUtil.stringArg;
import static com.conveyal.gtfs.graphql.fetchers.JDBCFetcher.makeInClause;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;

/**
 * This fetcher creates one big query by joining tables in the order specified in the initial definition.  The data
 * returned will only consist of the columns from the final table definition.  All other tables are assumed to be
 * intermediate tables used to create joins to obtain filtered data from the final table.  Also, any grqphQL arguments
 * are applied only to the final table.
 */
public class NestedJDBCFetcher implements DataFetcher<Object> {

    private static final Logger LOG = LoggerFactory.getLogger(NestedJDBCFetcher.class);
    private final JDBCFetcher[] jdbcFetchers;

    // Supply an SQL result row -> Object transformer

    /**
     * Constructor for data that must be derived by chaining multiple joins together.
     */
    public NestedJDBCFetcher (JDBCFetcher ...jdbcFetchers) {
        this.jdbcFetchers = jdbcFetchers;
    }

    // NestedJDBCFetcher should only be used in conjunction with more than one JDBCFetcher in order to chain multiple
    // joins together. As seen below, the type of the field must match the final JDBCFetcher table type (routeType ->
    // routes).
    public static GraphQLFieldDefinition field (String tableName) {
        return newFieldDefinition()
                .name(tableName)
                // Field type should be equivalent to the final JDBCFetcher table type.
                .type(new GraphQLList(GraphQLGtfsSchema.routeType))
                .argument(stringArg("namespace"))
                // Optional argument should match only the first join field.
                .argument(multiStringArg("stop_id"))
                .dataFetcher(new NestedJDBCFetcher(
                        // Auto limit is set to false in first fetchers because otherwise the limit could cut short the
                        // queries which first get the fields to join to routes.
                        new JDBCFetcher("pattern_stops", "stop_id", null, false),
                        new JDBCFetcher("patterns", "pattern_id", null, false),
                        new JDBCFetcher("routes", "route_id")))
                .build();
    }

    /**
     * get the data for the nested query by making a sql query.  The sql query is constructed by adding tables to be
     * queried and joining them via where clauses.  This ultimately returns a CompleteableFuture value that is
     * executed by graphQL at some strategic point in time.
     */
    @Override
    public Object get (DataFetchingEnvironment environment) {
        // GetSource is the context in which this this DataFetcher has been created, in this case a map representing
        // the parent feed (FeedFetcher).
        Map<String, Object> parentEntityMap = environment.getSource();

        // This DataFetcher only makes sense when the enclosing parent object is a feed or something in a feed.
        // So it should always be represented as a map with a namespace key.
        String namespace = (String) parentEntityMap.get("namespace");

        // Arguments are only applied for the final fetcher iteration.
        // FIXME: NestedJDBCFetcher may need to be refactored so that it avoids conventions of JDBCFetcher (like the
        // implied limit of 50 records). For now, the autoLimit field has been added to JDBCFetcher, so that certain
        // fetchers (like the nested ones used solely for joins here) will not apply the limit by default.
        Map<String, Object> graphQLQueryArguemnts = environment.getArguments();;

        JDBCFetcher lastFetcher = null;
        List<String> preparedStatementParameters = new ArrayList<>();
        Set<String> fromTables = new HashSet<>();
        List<String> whereConditions = new ArrayList<>();
        for (int i = 0; i < jdbcFetchers.length; i++) {
            JDBCFetcher fetcher = jdbcFetchers[i];
            String tableName = namespacedTableName(namespace, fetcher.tableName);
            if (i == 0) {
                // For first iteration of fetching, use parent entity to get in clause values.
                // Also, we add this directly to conditions since the table and field will be different than in the
                // final fetcher.
                List<String> inClauseValues = new ArrayList<>();
                Map<String, Object> enclosingEntity = environment.getSource();
                String inClauseValue = (String) enclosingEntity.get(fetcher.parentJoinField);
                // Check for null parentJoinValue to protect against NPE.
                if (inClauseValue == null) {
                    return new ArrayList<>();
                } else {
                    inClauseValues.add(inClauseValue);
                }
                // add the base table and in clause that this whole query is based off of.  We specific the namespaced table field
                // name to avoid conflicts resulting from selecting from other similarly named table fields
                fromTables.add(tableName);
                whereConditions.add(makeInClause(
                    namespacedTableFieldName(namespace, fetcher.tableName, fetcher.parentJoinField),
                    inClauseValues,
                    preparedStatementParameters
                ));
            } else {
                // add nested table join by using a where condition
                fromTables.add(tableName);
                whereConditions.add(String.format(
                    "%s = %s",
                    namespacedTableFieldName(namespace, lastFetcher.tableName, fetcher.parentJoinField),
                    namespacedTableFieldName(namespace, fetcher.tableName, fetcher.parentJoinField)
                ));
                // check if we have reached the final nested table
                if (i == jdbcFetchers.length - 1) {
                    // final nested table reached!
                    // create a sqlBuilder to select all values from the final table
                    StringBuilder sqlStatementStringBuilder = new StringBuilder();
                    sqlStatementStringBuilder.append("select ");
                    sqlStatementStringBuilder.append(
                        namespacedTableFieldName(namespace, fetcher.tableName, "*")
                    );
                    // Make the query and return the results!
                    return fetcher.getResults(
                        getJdbcQueryDataLoaderFromContext(environment),
                        getDataSourceFromContext(environment),
                        namespace,
                        new ArrayList<>(),
                        graphQLQueryArguemnts,
                        preparedStatementParameters,
                        whereConditions,
                        fromTables,
                        sqlStatementStringBuilder
                    );
                }
            }
            lastFetcher = fetcher;
        }
        // This piece of code will never be reached because of how things get returned above.
        // But it's here to make java happy.
        return new ArrayList<>();
    }
}
