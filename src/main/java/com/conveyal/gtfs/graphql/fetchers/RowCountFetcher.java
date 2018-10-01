package com.conveyal.gtfs.graphql.fetchers;

import com.conveyal.gtfs.graphql.GTFSGraphQL;
import com.conveyal.gtfs.loader.JDBCTableReader;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLObjectType;
import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.conveyal.gtfs.graphql.GraphQLUtil.intt;
import static com.conveyal.gtfs.graphql.GraphQLUtil.string;
import static com.conveyal.gtfs.graphql.GraphQLUtil.stringArg;
import static com.conveyal.gtfs.graphql.fetchers.JDBCFetcher.filterEquals;
import static graphql.Scalars.GraphQLInt;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;
import static graphql.schema.GraphQLObjectType.newObject;

/**
 * Get quantity of rows in the given table.
 */
public class RowCountFetcher implements DataFetcher {

    public static final Logger LOG = LoggerFactory.getLogger(RowCountFetcher.class);

    private final String tableName;
    // Filter field is optionally dynamic if an entity ID argument is provided for the groupByField.
    private final String filterField;
    private final String groupByField;

    public RowCountFetcher(String tableName) {
        this(tableName, null, null);
    }

    public RowCountFetcher(String tableName, String filterField) {
        this(tableName, filterField, null);
    }

    public RowCountFetcher(String tableName, String filterField, String groupByField) {
        this.tableName = tableName;
        this.filterField = filterField;
        this.groupByField = groupByField;
    }

    @Override
    public Object get(DataFetchingEnvironment environment) {
        Map<String, Object> parentFeedMap = environment.getSource();
        Map<String, Object> arguments = environment.getArguments();
        DataSource dataSource = GTFSGraphQL.getDataSourceFromContext(environment);
        List<String> argKeys = new ArrayList<>(arguments.keySet());
        List<String> parameters = new ArrayList<>();
        String namespace = (String) parentFeedMap.get("namespace");
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            List<String> fields = new ArrayList<>();
            fields.add("count(*)");
            List<String> clauses = new ArrayList<>();
            if (filterField != null) {
                // FIXME Does this handle null cases?
                // Add where clause to filter out non-matching results.
                clauses.add(
                    String.join(
                        " ",
                        "where",
                        filterEquals(filterField, (String) parentFeedMap.get(filterField), parameters)
                    )
                );
            } else if (groupByField != null) {
                // Handle group by field and optionally handle any filter arguments passed in.
                if (!argKeys.isEmpty()) {
                    // If a filter arg is provided, add a filter clause if the below conditions are not triggered.
                    if (argKeys.size() > 1) {
                        throw new IllegalStateException("Only one 'filter by' argument may be used with group field.");
                    }
                    String groupedFilterField = argKeys.get(0);
                    if (groupedFilterField.equals(groupByField)) {
                        throw new IllegalStateException("'Filter by' argument must not use same field as group field.");
                    }
                    String filterValue = (String) arguments.get(groupedFilterField);
                    if (filterValue != null) {
                        clauses.add(
                            String.join(
                                " ",
                                "where",
                                filterEquals(groupedFilterField, filterValue, parameters)
                            )
                        );
                    }
                }
                // Finally, add group by clause.
                fields.add(groupByField);
                clauses.add(String.format("group by %s", groupByField));
            }
            String sql = String.format("select %s from %s.%s %s",
                    String.join(", ", fields),
                    namespace,
                    tableName,
                    String.join(" ", clauses)
            );
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            int oneBasedIndex = 1;
            for (String parameter : parameters) {
                preparedStatement.setString(oneBasedIndex++, parameter);
            }
            LOG.info(preparedStatement.toString());
            if (preparedStatement.execute()) {
                ResultSet resultSet = preparedStatement.getResultSet();
                if (groupByField == null) {
                    // If not providing grouped counts, simply return the count value.
                    resultSet.next();
                    return resultSet.getInt(1);
                } else {
                    // Otherwise, accumulate group count objects into a list and return.
                    List<GroupCount> results = new ArrayList<>();
                    while(resultSet.next()) {
                        results.add(new GroupCount(resultSet.getString(2), resultSet.getInt(1)));
                    }
                    return results;
                }
            }
        } catch (SQLException e) {
            // In case the table doesn't exist in this feed, just return zero and don't print noise to the log.
            // Unfortunately JDBC doesn't seem to define reliable error codes.
            if (! JDBCTableReader.SQL_STATE_UNDEFINED_TABLE.equals(e.getSQLState())) {
                e.printStackTrace();
            }
        } finally {
            DbUtils.closeQuietly(connection);
        }
        return 0;
    }

    /**
     * Convenience method to create a field in a GraphQL schema that fetches the number of rows in a table.
     * Must be on a type that has a "namespace" field for context.
     */
    public static GraphQLFieldDefinition field (String fieldName, String tableName) {
        return newFieldDefinition()
                .name(fieldName)
                .type(GraphQLInt)
                .dataFetcher(new RowCountFetcher(tableName))
                .build();
    }

    /**
     * Convenience method to create a field in a GraphQL schema that fetches the number of rows in a table with a filter
     * or where clause. If a filter field is provided, count only the rows that match the parent entity's value for the
     * given field. For example, adding a trip_count field to routes (filter field route_id) would add a trip count to
     * each route entity with the number of trips that operate under each route's route_id.
     */
    public static GraphQLFieldDefinition field (String fieldName, String tableName, String filterField) {
        return newFieldDefinition()
                .name(fieldName)
                .type(GraphQLInt)
                .dataFetcher(new RowCountFetcher(tableName, filterField))
                .build();
    }

    /**
     * For cases where the GraphQL field name is the same as the table name itself.
     */
    public static GraphQLFieldDefinition field (String tableName) {
        return field(tableName, tableName);
    }


    /**
     * This is very similar to an {@link com.conveyal.gtfs.graphql.GraphQLGtfsSchema#errorCountType}, but is used for
     * counts of entities grouped by some column. It is defined here to ensure that it is statically
     * initialized before its use below.
     */
    public static GraphQLObjectType groupCountType = newObject().name("groupCount")
            .description("Quantity of entities all having the same value for a particular column.")
            .field(string("type"))
            .field(intt("count"))
            .build();

    /**
     * The convenience object corresponding to the groupCountType GraphQL type.
     */
    public static class GroupCount {
        public String type;
        public int count;

        public GroupCount(String type, int count) {
            this.type = type;
            this.count = count;
        }
    }

    /**
     * A GraphQL field used to deliver lists of group counts. Currently the primary use of this field is for delivering
     * trip counts grouped by either service_id, pattern_id, or route_id. These quantities can also be filtered by
     * providing a string value for pattern_id
     */
    public static GraphQLFieldDefinition groupedField(String tableName, String groupByColumn) {
        return newFieldDefinition()
                .name(groupByColumn)
                .type(groupCountType)
                .argument(stringArg("pattern_id"))
                .type(new GraphQLList(groupCountType))
                .dataFetcher(new RowCountFetcher(tableName, null, groupByColumn))
                .build();
    }
}