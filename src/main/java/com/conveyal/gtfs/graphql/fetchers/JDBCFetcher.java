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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    // Symbolic constants for argument names used to prevent misspellings.
    public static final String ID_ARG = "id";
    public static final String LIMIT_ARG = "limit";
    public static final String OFFSET_ARG = "offset";
    public static final String SEARCH_ARG = "search";
    public static final String DATE_ARG = "date";
    public static final String FROM_ARG = "from";
    public static final String TO_ARG = "to";
    // Lists of column names to be used when searching for string matches in the respective tables.
    public static final List<String> stopSearchColumns = Arrays.asList("stop_id", "stop_code", "stop_name");
    public static final List<String> routeSearchColumns = Arrays.asList("route_id", "route_short_name", "route_long_name");
    // The following lists of arguments are considered non-standard, i.e., they are not handled by filtering entities
    // with a simple WHERE clause. They are all bundled together in argsToSkip as a convenient way to pass over them
    // when constructing said WHERE clause.
    public static final List<String> boundingBoxArgs = Arrays.asList("minLat", "minLon", "maxLat", "maxLon");
    public static final List<String> dateTimeArgs = Arrays.asList("date", "from", "to");
    public static final List<String> otherNonStandardArgs = Arrays.asList(SEARCH_ARG, LIMIT_ARG, OFFSET_ARG);
    public static final List<String> argsToSkip = Stream.of(boundingBoxArgs, dateTimeArgs, otherNonStandardArgs)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
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
        Set<String> fromTables = new HashSet<>();
        // By default, select only from the primary table. Other tables may be added to this list to handle joins.
        fromTables.add(String.join(".", namespace, tableName));
        sqlBuilder.append("select *");

        // We will build up additional sql clauses in this List.
        Set<String> conditions = new HashSet<>();
        // The order by clause will go here.
        String sortBy = "";
        // TODO: Track the current parameter index for setting prepared statement parameters?
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
            // The pagination, bounding box, and date/time args should all be skipped here because they are handled
            // separately below from standard args (pagination becomes limit/offset clauses, bounding box applies to
            // stops table, and date/time args filter stop times. All other args become "where X in A, B, C" clauses.
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
            // Handle bounding box arguments if ALL are supplied. The stops falling within the bounds will be returned.
            // If operating on the stops table, this will just add the bounds filters to the conditions list. If
            // operating on the patterns table, a SELECT DISTINCT patterns query will be constructed with a join to
            // stops and pattern stops.
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
        if (argumentKeys.containsAll(dateTimeArgs)) {
            // Handle filtering stop_times by date and period of time (e.g., 4/12/2018 8:00am-11:00am). NOTE: time args
            // are actually specified in seconds since midnight.
            // NOTE: This query must be run only on feeds that have run through validation, which generates the
            // service_dates table. In other words, feeds generated by the editor cannot be queried with the date/time args.
            fromTables.add(String.format("%s.trips", namespace));
            String date = (String) arguments.get(DATE_ARG);
            // Gather all service IDs that run on the provided date.
            fromTables.add(String.format("(select distinct service_id from %s.service_dates where service_date = '%s') as unique_service_ids_in_operation",
                    namespace, date));
            // Departure time must be greater than or equal to "from" argument (start of time period)
            conditions.add(String.format("%s.stop_times.departure_time >= %d", namespace, (int) arguments.get(FROM_ARG)));
            // Arrival time must be less than or equal to "to" argument (end of time period)
            // FIXME: Should the end of the time window filter on departure time instead arrival time?
            conditions.add(String.format("%s.stop_times.arrival_time <= %d", namespace, (int) arguments.get(TO_ARG)));
            // The following conditions join stop times to trips and trips to service_dates (unique_service_ids_in_operation).
            conditions.add(String.format("%s.stop_times.trip_id = %s.trips.trip_id", namespace, namespace));
            conditions.add(String.format("%s.trips.service_id = unique_service_ids_in_operation.service_id", namespace));
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
