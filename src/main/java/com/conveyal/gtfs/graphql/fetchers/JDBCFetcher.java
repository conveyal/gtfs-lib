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
public class JDBCFetcher implements DataFetcher<List<Map<String, Object>>> {

    public static final Logger LOG = LoggerFactory.getLogger(JDBCFetcher.class);

    // Make this an option to the GraphQL query.
    public static final int DEFAULT_ROWS_TO_FETCH = 50;
    public static final int MAX_ROWS_TO_FETCH = 500;

    public static final String ID_ARG = "id";
    public static final String LIMIT_ARG = "limit";
    public static final String OFFSET_ARG = "offset";
    public static final String SEARCH_ARG = "search";
    public static final List<String> stopSearchColumns = Arrays.asList("stop_id", "stop_code", "stop_name");
    public static final List<String> routeSearchColumns = Arrays.asList("route_id", "route_short_name", "route_long_name");
    public static final List<String> boundingBoxArgs = Arrays.asList("minLat", "minLon", "maxLat", "maxLon");
    // FIXME: Search arg is not a pagination arg, but it's listed here because it should not be treated like standard
    // args.
    public static final List<String> paginationArgs = Arrays.asList(SEARCH_ARG, LIMIT_ARG, OFFSET_ARG);
    public final String tableName;
    public final String parentJoinField;
    private final String sortField;
    private final boolean autoLimit;

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
        this(tableName, parentJoinField, null, true);
    }

    /**
     *
     * @param sortField The field on which to sort the list or fetched rows (in ascending order only).
     *                  If null, no sort is included.
     * @param autoLimit Whether to by default apply a limit to the fetched rows. This is used for certain
     *                  tables that it is unnatural to expect a limit (e.g., shape points or pattern stops).
     */
    public JDBCFetcher (String tableName, String parentJoinField, String sortField, boolean autoLimit) {
        this.tableName = tableName;
        this.parentJoinField = parentJoinField;
        this.sortField = sortField;
        this.autoLimit = autoLimit;
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
        Set<String> fromTables = new HashSet<>();
        // By default, select only from the primary table. Other tables may be added to this list to handle joins.
        fromTables.add(String.join(".", namespace, tableName));
        sqlBuilder.append("select *");

        // We will build up additional sql clauses in this List.
        Set<String> conditions = new HashSet<>();
        // The order by clause will go here.
        String sortBy = "";
        // Track the current parameter index for setting prepared statement parameters
        int parameterIndex = 1;
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
        Set<String> argumentKeys = arguments.keySet();
        for (String key : argumentKeys) {
            // Limit and Offset arguments are for pagination. projectStops is used to mutate shape points before
            // returning them. All others become "where X in A, B, C" clauses.
            Set<String> argsToSkip = new HashSet<>();
            argsToSkip.addAll(boundingBoxArgs);
            argsToSkip.addAll(paginationArgs);
            if (argsToSkip.contains(key)) continue;
            if (ID_ARG.equals(key)) {
                Integer value = (Integer) arguments.get(key);
                conditions.add(String.join(" = ", "id", value.toString()));
            } else {
                List<String> values = (List<String>) arguments.get(key);
                if (values != null && !values.isEmpty()) conditions.add(makeInClause(key, values));
            }
        }
        if (argumentKeys.containsAll(boundingBoxArgs)) {
            Set<String> boundsConditions = new HashSet<>();
            // Handle bounding box arguments if ALL are supplied.
            // NOTE: This is currently only defined for stops, but can be applied to patterns through a join
            for (String bound : boundingBoxArgs) {
                Double value = (Double) arguments.get(bound);
                // Determine delimiter/equality operator based on min/max
                String delimiter = bound.startsWith("max") ? " <= " : " >= ";
                // Determine field based on lat/lon
                String field = bound.toLowerCase().endsWith("lon") ? "stop_lon" : "stop_lat";
                // Scope field with namespace and table name
                String fieldWithNamespace = String.join(".", namespace, "stops", field);
                boundsConditions.add(String.join(delimiter, fieldWithNamespace, value.toString()));
            }
            if ("stops".equals(tableName)) {
                conditions.addAll(boundsConditions);
            } else if ("patterns".equals(tableName)) {
                // Add from table as unique_pattern_ids_in_bounds to match patterns table -> pattern stops -> stops
                fromTables.add(
                        String.format(
                                "(select distinct ps.pattern_id from %s.stops, %s.pattern_stops as ps where %s AND %s.stops.stop_id = ps.stop_id) as unique_pattern_ids_in_bounds",
                                namespace,
                                namespace,
                                String.join(" and ", boundsConditions),
                                namespace
                        ));
                conditions.add(String.format("%s.patterns.pattern_id = unique_pattern_ids_in_bounds.pattern_id", namespace));
            }
        }
        if (argumentKeys.contains(SEARCH_ARG)) {
            // Handle string search argument
            String value = (String) arguments.get(SEARCH_ARG);
            if (!value.isEmpty()) {
                // Only apply string search if string is not empty.
                Set<String> searchFields = getSearchFields(namespace);
                List<String> searchClauses = new ArrayList<>();
                for (String field : searchFields) {
                    // Double percent signs format as single percents, which are used for the string matching.
                    // FIXME: sanitize for SQL injection!!
                    // FIXME: is ILIKE compatible with non-Postgres? LIKE doesn't work well enough (even when setting
                    // the strings to lower case.
                    searchClauses.add(String.format("%s ILIKE '%%%s%%'", field, value));
                }
                if (!searchClauses.isEmpty()) {
                    // Wrap string search in parentheses to isolate from other conditions.
                    conditions.add(String.format(("(%s)"), String.join(" OR ", searchClauses)));
                }
            }
        }
        sqlBuilder.append(String.format(" from %s", String.join(", ", fromTables)));
        if (!conditions.isEmpty()) {
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
        if (limit == -1 || !autoLimit) {
            // Do not append limit if explicitly set to -1 or autoLimit is disabled. NOTE: this conditional block is
            // empty simply because it is clearer to define the condition in this way (vs. if limit > 0).
            // FIXME: Skipping limit is not scalable in many cases and should possibly be removed/limited.
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
//            LOG.info("table name={}", tableName);
//            LOG.info("SQL: {}", sqlBuilder.toString());
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

    /**
     * Get the list of fields to perform a string search on for the provided table. Each table included here must have
     * the {@link #SEARCH_ARG} argument applied in the query definition.
     */
    private Set<String> getSearchFields(String namespace) {
        // Check table metadata for presence of columns.
//        long startTime = System.currentTimeMillis();
        Set<String> columnsForTable = new HashSet<>();
        List<String> searchColumns = new ArrayList<>();
        Connection connection = null;
        try {
            connection = GTFSGraphQL.getConnection();
            ResultSet columns = connection.getMetaData().getColumns(null, namespace, tableName, null);
            while (columns.next()) {
                String column = columns.getString(4);
                columnsForTable.add(column);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DbUtils.closeQuietly(connection);
        }
        // Each query seems to take between 10 and 30 milliseconds to get column names. This seems acceptable to avoid
        // errors for conditions that include columns that don't exist.
//        LOG.info("Took {} ms to get column name metadata.", System.currentTimeMillis() - startTime);
        switch (tableName) {
            case "stops":
                searchColumns = stopSearchColumns;
                break;
            case "routes":
                searchColumns = routeSearchColumns;
                break;
            // TODO: add string search on other tables? For example, trips: id, headway; agency: name; patterns: name.
            default:
                // Search columns will be an empty set, which will ultimately return an empty set.
                break;
        }
        // Filter available columns in table by search columns.
        columnsForTable.retainAll(searchColumns);
        return columnsForTable;
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
//        ESAPI.encoder().encodeForSQL( new PostgreSQLCodec(), string);
        sb.append(string);
        sb.append("'");
    }

    private String quote (String string) {
        StringBuilder sb = new StringBuilder();
        quote(sb, string);
        return sb.toString();
    }

}
