package com.conveyal.gtfs.graphql.fetchers;

import com.conveyal.gtfs.graphql.GraphQLGtfsSchema;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.conveyal.gtfs.graphql.GraphQLUtil.multiStringArg;
import static com.conveyal.gtfs.graphql.GraphQLUtil.stringArg;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;

/**
 * This fetcher uses nested calls to the general JDBCFetcher for SQL data. It uses the parent entity and a series of
 * joins to leap frog from one entity to another more distantly-related entity. For example, if we want to know the
 * routes that serve a specific stop, starting with a top-level stop type, we can nest joins from stop ABC -> pattern
 * stops -> patterns -> routes (see below example implementation for more details).
 */
public class NestedJDBCFetcher implements DataFetcher<List<Map<String, Object>>> {

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

    @Override
    public List<Map<String, Object>> get (DataFetchingEnvironment environment) {
        // Store the join values here.
        ListMultimap<String, String> joinValuesForJoinField = MultimapBuilder.treeKeys().arrayListValues().build();

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
        Map<String, Object> arguments = null;

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
            } else {
                // Otherwise, get join values stored by previous fetcher.
                joinValues = joinValuesForJoinField.get(fetcher.parentJoinField);
                if (i == jdbcFetchers.length - 1) {
                    // Apply arguments only to the final fetched table
                    arguments = environment.getArguments();
                    LOG.info("{} args: {}", fetcher.tableName, arguments.keySet().toString());
                }
            }
            LOG.info("Join values: {}", joinValues.toString());
            fetchResults = fetcher.getResults(namespace, joinValues, arguments);
            if (fetchResults.size() == 0) {
                // If there are no results, the following queries will have no results to join to, so we can simply
                // return the empty list.
                return fetchResults;
            } else if (i < jdbcFetchers.length - 1) {
                // Otherwise, iterate over results from current fetcher to store for next iteration for all but the last
                // iteration (the last iteration's fetchResults will contain the final results).
                JDBCFetcher nextFetcher = jdbcFetchers[i + 1];
                for (Map<String, Object> entity : fetchResults) {
                    Object joinValue = entity.get(nextFetcher.parentJoinField);
                    // Store join values in multimap for
                    if (joinValue != null)
                        joinValuesForJoinField.put(nextFetcher.parentJoinField, joinValue.toString());
                }
            }
        }
        // Last iteration should finally return results.
        return fetchResults;
    }
}
