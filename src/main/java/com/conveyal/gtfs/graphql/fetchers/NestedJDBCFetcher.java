package com.conveyal.gtfs.graphql.fetchers;

import com.conveyal.gtfs.graphql.GTFSGraphQL;
import com.conveyal.gtfs.graphql.GraphQLGtfsSchema;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.conveyal.gtfs.graphql.GraphQLUtil.multiStringArg;
import static com.conveyal.gtfs.graphql.GraphQLUtil.stringArg;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;

/**
 * A generic fetcher to get fields out of an SQL database table.
 */
public class NestedJDBCFetcher implements DataFetcher<List<Map<String, Object>>> {

    public static final Logger LOG = LoggerFactory.getLogger(NestedJDBCFetcher.class);

    // Make this an option to the GraphQL query.
    public static final int DEFAULT_ROWS_TO_FETCH = 50;
    public static final int MAX_ROWS_TO_FETCH = 500;

    private final JDBCFetcher[] jdbcFetchers;

    // Supply an SQL result row -> Object transformer

    /**
     * Constructor for tables that need neither restriction by a where clause nor sorting based on the enclosing entity.
     * These would typically be at the topmost level, directly inside a feed rather than nested in some GTFS entity type.
     */
    public NestedJDBCFetcher (JDBCFetcher ...jdbcFetchers) {
        this.jdbcFetchers = jdbcFetchers;
    }

    // We can't automatically generate JDBCFetcher based field definitions for inclusion in a GraphQL schema (as we
    // do for MapFetcher for example). This is because we need custom inclusion of sub-tables in each table type.
    // Still maybe we could make the most basic ones this way (automatically). Keeping this function as an example.
    public static GraphQLFieldDefinition field (String tableName) {
        return newFieldDefinition()
                .name(tableName)
                // Field type should be equivalent to the final JDBCFetcher table type.
                .type(new GraphQLList(GraphQLGtfsSchema.routeType))
                .argument(stringArg("namespace"))
                // Optional argument should match only the first join field.
                .argument(multiStringArg("stop_id"))
                .dataFetcher(new NestedJDBCFetcher(
                        new JDBCFetcher("pattern_stops", "stop_id"),
                        new JDBCFetcher("patterns", "pattern_id"),
                        new JDBCFetcher("routes", "route_id")))
                .build();
    }

    // Horrifically, we're going from SQL response to Gtfs-lib Java model object to GraphQL Java object to JSON.
    // What if we did direct SQL->JSON?
    // Could we transform JDBC ResultSets directly to JSON?
    // With Jackson streaming API we can make a ResultSet serializer: https://stackoverflow.com/a/8120442

    // We could apply a transformation from ResultSet to Gtfs-lib model object, but then more DataFetchers
    // need to be defined to pull the fields out of those model objects. I'll try to skip those intermediate objects.

    // Unfortunately we can't just apply DataFetchers directly to the ResultSets because they're cursors, and we'd
    // have to somehow advance them at the right moment. So we need to transform the SQL results into fully materialized
    // Java objects, then transform those into GraphQL fields. Fortunately the final transformation is trivial fetching
    // from a Map<String, Object>.
    // But what are the internal GraphQL objects, i.e. what does an ExecutionResult return? Are they Map<String, Object>?

    @Override
    public List<Map<String, Object>> get (DataFetchingEnvironment environment) {
        // Store the join values here.
        ListMultimap<String, String> joinValuesForJoinField = MultimapBuilder.treeKeys().arrayListValues().build();

        // GetSource is the context in which this this DataFetcher has been created, in this case a map representing
        // the parent feed (FeedFetcher).
        Map<String, Object> parentEntityMap = environment.getSource();

        // Apparently you can't get the arguments from the parent - how do you have arguments on sub-fields?
        // It looks like you have to redefine the argument on each subfield and pass it explicitly in the Graphql request.

        // String namespace = environment.getArgument("namespace"); // This is going to be the unique prefix, not the feedId in the usual sense
        // This DataFetcher only makes sense when the enclosing parent object is a feed or something in a feed.
        // So it should always be represented as a map with a namespace key.

        String namespace = (String) parentEntityMap.get("namespace");

        Map<String, Object> arguments;

        // Store each iteration's fetch results here.
        List<Map<String, Object>> fetchResults = null;
        for (int i = 0; i < jdbcFetchers.length; i++) {
            JDBCFetcher fetcher = jdbcFetchers[i];
            List<String> joinValues;
            if (i == 0) {
                // For first iteration of fetching, use parent entity to get join values.
                Map<String, Object> enclosingEntity = environment.getSource();
                // FIXME SQL injection: enclosing entity's ID could contain malicious character sequences; quote and sanitize the string.
                // FIXME: THIS IS BROKEN if parentJoinValue is null!!!!
                Object parentJoinValue = enclosingEntity.get(fetcher.parentJoinField);
                // Check for null parentJoinValue to protect against NPE.
                joinValues = new ArrayList<>();
                String parentJoinString = parentJoinValue == null ? null : parentJoinValue.toString();
                joinValues.add(parentJoinString);
                if (parentJoinValue == null) {
                    return new ArrayList<>();
                }
                // Arguments match parent entity
                arguments = environment.getArguments();
                // FIXME: should we limit
                LOG.info(arguments.keySet().toString());
                LOG.info(arguments.values().toString());
//                if (arguments.keySet().isEmpty()) return new ArrayList<>();
            } else {
                // Otherwise, get join values stored by previous fetcher.
                joinValues = joinValuesForJoinField.get(fetcher.parentJoinField);
                // Arguments are null if not the first iteration.
                arguments = null;
            }
            LOG.info(joinValues.toString());
            fetchResults = fetcher.getResults(namespace, joinValues, arguments);
            if (i < jdbcFetchers.length - 1) {
                JDBCFetcher nextFetcher = jdbcFetchers[i + 1];
                // Iterate over results from fetcher to store for next iteration.
                for (Map<String, Object> entity : fetchResults) {
                    Object joinValue = entity.get(nextFetcher.parentJoinField);
                    // Store join values in multimap for
                    if (joinValue != null) joinValuesForJoinField.put(nextFetcher.parentJoinField, joinValue.toString());
                }
            }
        }
        // Last iteration should finally return results.
        return fetchResults;
    }
}
