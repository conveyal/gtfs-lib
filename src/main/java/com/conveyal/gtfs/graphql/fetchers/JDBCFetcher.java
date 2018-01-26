package com.conveyal.gtfs.graphql.fetchers;

import com.conveyal.gtfs.graphql.GTFSGraphQL;
import com.conveyal.gtfs.graphql.GraphQLGtfsSchema;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.conveyal.gtfs.graphql.GraphQLUtil.multiStringArg;
import static com.conveyal.gtfs.graphql.GraphQLUtil.stringArg;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;

/**
 * A generic fetcher to get fields out of an SQL database table.
 */
public class JDBCFetcher implements DataFetcher<List<Map<String, Object>>> {

    public static final Logger LOG = LoggerFactory.getLogger(JDBCFetcher.class);

    // Make this an option to the GraphQL query.
    public static final int DEFAULT_ROWS_TO_FETCH = 50;
    public static final int MAX_ROWS_TO_FETCH = 500;

    public static final String ID_ARG = "id";
    public static final String LIMIT_ARG = "limit";
    public static final String OFFSET_ARG = "offset";
    public final String tableName;
    public final String parentJoinField;
    private final String sortField;

    // Supply an SQL result row -> Object transformer

    /**
     * Constructor for tables that need neither restriction by a where clause nor sorting based on the enclosing entity.
     * These would typically be at the topmost level, directly inside a feed rather than nested in some GTFS entity type.
     */
    public JDBCFetcher (String tableName) {
        this(tableName, null);
    }

    /**
     * @param tableName the database table from which to fetch rows.
     * @param parentJoinField The field in the enclosing level of the Graphql query to use in a where clause.
     *        This allows e.g. selecting all the stop_times within a trip, using the enclosing trip's trip_id.
     *        If null, no such clause is added.
     */
    public JDBCFetcher (String tableName, String parentJoinField) {
        this(tableName, parentJoinField, null);
    }

    /**
     *
     * @param sortField The field on which to sort the list or fetched rows (in ascending order only).
     *                  If null, no sort is included.
     */
    public JDBCFetcher (String tableName, String parentJoinField, String sortField) {
        this.tableName = tableName;
        this.parentJoinField = parentJoinField;
        this.sortField = sortField;
    }

    // We can't automatically generate JDBCFetcher based field definitions for inclusion in a GraphQL schema (as we
    // do for MapFetcher for example). This is because we need custom inclusion of sub-tables in each table type.
    // Still maybe we could make the most basic ones this way (automatically). Keeping this function as an example.
    public static GraphQLFieldDefinition field (String tableName) {
        return newFieldDefinition()
                .name(tableName)
                .type(new GraphQLList(GraphQLGtfsSchema.routeType))
                .argument(stringArg("namespace"))
                .argument(multiStringArg("route_id"))
                .dataFetcher(new JDBCFetcher(tableName, null))
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
        // GetSource is the context in which this this DataFetcher has been created, in this case a map representing
        // the parent feed (FeedFetcher).
        Map<String, Object> parentEntityMap = environment.getSource();

        // Apparently you can't get the arguments from the parent - how do you have arguments on sub-fields?
        // It looks like you have to redefine the argument on each subfield and pass it explicitly in the Graphql request.

        // String namespace = environment.getArgument("namespace"); // This is going to be the unique prefix, not the feedId in the usual sense
        // This DataFetcher only makes sense when the enclosing parent object is a feed or something in a feed.
        // So it should always be represented as a map with a namespace key.

        String namespace = (String) parentEntityMap.get("namespace");

        // If we are fetching an item nested within a GTFS entity in the Graphql query, we want to add an SQL "where"
        // clause using the values found here. Note, these are used in the below getResults call.
        List<String> parentJoinValues = new ArrayList<>();
        if (parentJoinField != null) {
            Map<String, Object> enclosingEntity = environment.getSource();
            // FIXME SQL injection: enclosing entity's ID could contain malicious character sequences; quote and sanitize the string.
            // FIXME: THIS IS BROKEN if parentJoinValue is null!!!!
            Object parentJoinValue = enclosingEntity.get(parentJoinField);
            // Check for null parentJoinValue to protect against NPE.
            String parentJoinString = parentJoinValue == null ? null : parentJoinValue.toString();
            parentJoinValues.add(parentJoinString);
            if (parentJoinValue == null) {
                return new ArrayList<>();
            }
        }
        Map<String, Object> arguments = environment.getArguments();

        return getResults(namespace, parentJoinValues, arguments);
    }

    /**
     * Handle fetching functionality for a given namespace, set of join values, and arguments. This is broken out from
     * the standard get function so that it can be reused in other fetchers (i.e., NestedJdbcFetcher)
     */
    public List<Map<String, Object>> getResults (String namespace, List<String> parentJoinValues, Map<String, Object> arguments) {
        // This will contain one Map<String, Object> for each row fetched from the database table.
        List<Map<String, Object>> results = new ArrayList<>();
        if (arguments == null) arguments = new HashMap<>();
        if (namespace == null) {
            // If namespace is null, do no attempt a query on a namespace that does not exist.
            return null;
        }
        StringBuilder sqlBuilder = new StringBuilder();

        // We could select only the requested fields by examining environment.getFields(), but we just get them all.
        // The advantage of selecting * is that we don't need to validate the field names.
        // All the columns will be loaded into the Map<String, Object>,
        // but only the requested fields will be fetched from that Map using a MapFetcher.
        sqlBuilder.append(String.format("select * from %s.%s", namespace, tableName));

        // We will build up additional sql clauses in this List.
        List<String> conditions = new ArrayList<>();
        // The order by clause will go here.
        String sortBy = "";

        // If we are fetching an item nested within a GTFS entity in the Graphql query, we want to add an SQL "where"
        // clause. This could conceivably be done automatically, but it's clearer to just express the intent.
        // Note, this is assuming the type of the field in the parent is a string.
        if (parentJoinField != null && parentJoinValues != null && !parentJoinValues.isEmpty()) {
            // FIXME SQL injection: enclosing entity's ID could contain malicious character sequences; quote and sanitize the string.
            conditions.add(makeInClause(parentJoinField, parentJoinValues));
        }
        if (sortField != null) {
            // FIXME add sort order?
            sortBy = String.format(" order by %s", sortField);
        }
        for (String key : arguments.keySet()) {
            // Limit and Offset arguments are for pagination. projectStops is used to mutate shape points before
            // returning them. All others become "where X in A, B, C" clauses.
            String[] argsToSkip = new String[]{LIMIT_ARG, OFFSET_ARG};
            if (Arrays.asList(argsToSkip).indexOf(key) != -1) continue;
            if (ID_ARG.equals(key)) {
                Integer value = (Integer) arguments.get(key);
                conditions.add(String.join(" = ", "id", value.toString()));
            } else {
                List<String> values = (List<String>) arguments.get(key);
                if (values != null && !values.isEmpty()) conditions.add(makeInClause(key, values));
            }
        }
        if ( ! conditions.isEmpty()) {
            sqlBuilder.append(" where ");
            sqlBuilder.append(String.join(" and ", conditions));
        }
        // The default value for sortBy is an empty string, so it's safe to always append it here.
        sqlBuilder.append(sortBy);
        Integer limit = (Integer) arguments.get(LIMIT_ARG);
        if (limit == null) {
            limit = DEFAULT_ROWS_TO_FETCH;
        }
        if (limit > MAX_ROWS_TO_FETCH) {
            limit = MAX_ROWS_TO_FETCH;
        }
        // FIXME: Skipping limit is not scalable and should be removed.
        if (limit == -1) {
            // skip limit.
        } else {
            sqlBuilder.append(" limit " + limit);
        }
        Integer offset = (Integer) arguments.get(OFFSET_ARG);
        if (offset != null && offset >= 0) {
            sqlBuilder.append(" offset " + offset);
        }
        Connection connection = null;
        try {
            connection = GTFSGraphQL.getConnection();
            Statement statement = connection.createStatement();
            // This logging produces a lot of noise during testing due to large numbers of joined sub-queries
            // LOG.info("SQL: {}", sqlBuilder.toString());
            if (statement.execute(sqlBuilder.toString())) {
                ResultSet resultSet = statement.getResultSet();
                ResultSetMetaData meta = resultSet.getMetaData();
                int nColumns = meta.getColumnCount();
                // Iterate over result rows
                while (resultSet.next()) {
                    // Create a Map to hold the contents of this row, injecting the sql schema namespace into every map
                    Map<String, Object> resultMap = new HashMap<>();
                    resultMap.put("namespace", namespace);
                    // One-based iteration: start at one and use <=.
                    for (int i = 1; i <= nColumns; i++) {
                        resultMap.put(meta.getColumnName(i), resultSet.getObject(i));
                    }
                    results.add(resultMap);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DbUtils.closeQuietly(connection);
        }
        // Return a List of Maps, one Map for each row in the result.
        return results;
    }

    /** Construct filter clause with '=' (single string) or 'in' (multiple strings). */
    private String makeInClause(String key, List<String> strings) {
        StringBuilder sb = new StringBuilder();
        sb.append(key);
        if (strings.size() == 1) {
            sb.append(" = ");
            quote(sb, strings.get(0));
        } else {
            sb.append(" in (");
            for (int i = 0; i < strings.size(); i++) {
                if (i > 0) sb.append(",");
                quote(sb, strings.get(i));
            }
            sb.append(")");
        }
        return sb.toString();
    }

    // TODO SQL sanitization to avoid injection
    private void quote(StringBuilder sb, String string) {
        sb.append("'");
        sb.append(string);
        sb.append("'");
    }

    private String quote (String string) {
        StringBuilder sb = new StringBuilder();
        quote(sb, string);
        return sb.toString();
    }

}
