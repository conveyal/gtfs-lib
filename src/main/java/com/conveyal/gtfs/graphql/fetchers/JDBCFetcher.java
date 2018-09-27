package com.conveyal.gtfs.graphql.fetchers;

import com.conveyal.gtfs.graphql.GTFSGraphQL;
import com.conveyal.gtfs.graphql.GraphQLGtfsSchema;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import org.apache.commons.dbutils.DbUtils;
import org.dataloader.BatchLoaderEnvironment;
import org.dataloader.DataLoader;
import org.dataloader.MappedBatchLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.conveyal.gtfs.graphql.GraphQLUtil.multiStringArg;
import static com.conveyal.gtfs.graphql.GraphQLUtil.stringArg;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;

/**
 * A generic fetcher to get fields out of an SQL database table.
 */
public class JDBCFetcher implements DataFetcher<Object> {

    public static final Logger LOG = LoggerFactory.getLogger(JDBCFetcher.class);

    // Make this an option to the GraphQL query.
    private static final int DEFAULT_ROWS_TO_FETCH = 50;
    private static final int MAX_ROWS_TO_FETCH = 500;
    // Symbolic constants for argument names used to prevent misspellings.
    public static final String ID_ARG = "id";
    public static final String LIMIT_ARG = "limit";
    public static final String OFFSET_ARG = "offset";
    public static final String SEARCH_ARG = "search";
    public static final String DATE_ARG = "date";
    public static final String FROM_ARG = "from";
    public static final String TO_ARG = "to";
    public static final String MIN_LAT = "minLat";
    public static final String MIN_LON = "minLon";
    public static final String MAX_LAT = "maxLat";
    public static final String MAX_LON = "maxLon";
    // Lists of column names to be used when searching for string matches in the respective tables.
    private static final List<String> stopSearchColumns = Arrays.asList("stop_id", "stop_code", "stop_name");
    private static final List<String> routeSearchColumns = Arrays.asList("route_id", "route_short_name", "route_long_name");
    // The following lists of arguments are considered non-standard, i.e., they are not handled by filtering entities
    // with a simple WHERE clause. They are all bundled together in argsToSkip as a convenient way to pass over them
    // when constructing said WHERE clause.
    private static final List<String> boundingBoxArgs = Arrays.asList(MIN_LAT, MIN_LON, MAX_LAT, MAX_LON);
    private static final List<String> dateTimeArgs = Arrays.asList("date", "from", "to");
    private static final List<String> otherNonStandardArgs = Arrays.asList(SEARCH_ARG, LIMIT_ARG, OFFSET_ARG);
    private static final List<String> argsToSkip = Stream.of(boundingBoxArgs, dateTimeArgs, otherNonStandardArgs)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    public final String tableName;
    final String parentJoinField;
    private final String sortField;
    private final boolean autoLimit;

    public static final MappedBatchLoader<JdbcQuery, List<Map<String, Object>>> sqlBatchLoader =
        (jdbcQueries) -> CompletableFuture.supplyAsync(() -> getSqlQueryResults(jdbcQueries));

    private static Map<JdbcQuery, List<Map<String, Object>>> getSqlQueryResults(Set<JdbcQuery> jdbcQueries) {
        Map<JdbcQuery, List<Map<String, Object>>> allResults = new HashMap<>();
        Connection connection = null;
        try {
            connection = GTFSGraphQL.getConnection();
            for (JdbcQuery jdbcQuery : jdbcQueries) {
                allResults.put(jdbcQuery, getSqlQueryResult(jdbcQuery, connection));
            }
        } finally {
            DbUtils.closeQuietly(connection);
        }
        return allResults;
    }

    public static List<Map<String, Object>> getSqlQueryResult (JdbcQuery jdbcQuery, Connection connection) {
        try {
            List<Map<String, Object>> results = new ArrayList<>();
            PreparedStatement preparedStatement = connection.prepareStatement(jdbcQuery.sqlStatement);
            int oneBasedIndex = 1;
            for (String parameter : jdbcQuery.preparedStatementParameters) {
                preparedStatement.setString(oneBasedIndex++, parameter);
            }
            // This logging produces a lot of noise during testing due to large numbers of joined sub-queries
            //            LOG.info("table name={}", tableName);
            LOG.info("SQL: {}", preparedStatement.toString());
            if (preparedStatement.execute()) {
                ResultSet resultSet = preparedStatement.getResultSet();
                ResultSetMetaData meta = resultSet.getMetaData();
                int nColumns = meta.getColumnCount();
                // Iterate over result rows
                while (resultSet.next()) {
                    // Create a Map to hold the contents of this row, injecting the sql schema namespace into every map
                    Map<String, Object> resultMap = new HashMap<>();
                    resultMap.put("namespace", jdbcQuery.namespace);
                    // One-based iteration: start at one and use <=.
                    for (int i = 1; i <= nColumns; i++) {
                        resultMap.put(meta.getColumnName(i), resultSet.getObject(i));
                    }
                    results.add(resultMap);
                }
            }
            LOG.info("Result size: {}", results.size());
            // Return a List of Maps, one Map for each row in the result.
            return results;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static final DataLoader<JdbcQuery, List<Map<String, Object>>> jdbcDataLoader =
        DataLoader.newMappedDataLoader(sqlBatchLoader);

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
    public Object get (DataFetchingEnvironment environment) {
        // GetSource is the context in which this this DataFetcher has been created, in this case a map representing
        // the parent feed (FeedFetcher).
        Map<String, Object> parentEntityMap = environment.getSource();

        // Apparently you can't get the arguments from the parent - how do you have arguments on sub-fields?
        // It looks like you have to redefine the argument on each subfield and pass it explicitly in the GraphQL request.

        // String namespace = environment.getArgument("namespace"); // This is going to be the unique prefix, not the feedId in the usual sense
        // This DataFetcher only makes sense when the enclosing parent object is a feed or something in a feed.
        // So it should always be represented as a map with a namespace key.

        String namespace = (String) parentEntityMap.get("namespace");

        // If we are fetching an item nested within a GTFS entity in the GraphQL query, we want to add an SQL "where"
        // clause using the values found here. Note, these are used in the below getResults call.
        List<String> inClauseValues = new ArrayList<>();
        if (parentJoinField != null) {
            Map<String, Object> enclosingEntity = environment.getSource();
            // FIXME: THIS IS BROKEN if parentJoinValue is null!!!!
            Object inClauseValue = enclosingEntity.get(parentJoinField);
            // Check for null parentJoinValue to protect against NPE.
            String inClauseString = inClauseValue == null ? null : inClauseValue.toString();
            inClauseValues.add(inClauseString);
            if (inClauseValue == null) {
                return new ArrayList<>();
            }
        }
        Map<String, Object> arguments = environment.getArguments();

        return getResults(namespace, inClauseValues, arguments);
    }

    Object getResults (
        String namespace,
        List<String> inClauseValues,
        Map<String, Object> arguments
    ) {
        // We could select only the requested fields by examining environment.getFields(), but we just get them all.
        // The advantage of selecting * is that we don't need to validate the field names.
        // All the columns will be loaded into the Map<String, Object>,
        // but only the requested fields will be fetched from that Map using a MapFetcher.
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("select *");
        return getResults(
            namespace,
            inClauseValues,
            arguments,
            // No additional prepared sqlStatement preparedStatementParameters, where conditions or tables are needed beyond those added by
            // default in the next method.
            new ArrayList<>(),
            new ArrayList<>(),
            new HashSet<>(),
            sqlBuilder
        );
    }

    /**
     * Handle fetching functionality for a given namespace, set of join values, and arguments. This is broken out from
     * the standard get function so that it can be reused in other fetchers (i.e., NestedJdbcFetcher)
     */
    Object getResults (
        String namespace,
        List<String> inClauseValues,
        Map<String, Object> graphQLQueryArguments,
        List<String> preparedStatementParameters,
        List<String> whereConditions,
        Set<String> fromTables,
        StringBuilder sqlStatementStringBuilder
    ) {
        // This will contain one Map<String, Object> for each row fetched from the database table.
        List<Map<String, Object>> results = new ArrayList<>();
        if (graphQLQueryArguments == null) graphQLQueryArguments = new HashMap<>();
        // Ensure namespace exists and is clean. Note: FeedFetcher will have executed before this and validated that an
        // entry exists in the feeds table and the schema actually exists in the database.
        validateNamespace(namespace);

        // The primary table being selected from is added here. Other tables may have been / will be added to this list to handle joins.
        fromTables.add(String.join(".", namespace, tableName));

        // The order by clause will go here.
        String sortBy = "";
        // If we are fetching an item nested within a GTFS entity in the GraphQL query, we want to add an SQL "where"
        // clause. This could conceivably be done automatically, but it's clearer to just express the intent.
        // Note, this is assuming the type of the field in the parent is a string.
        if (parentJoinField != null && inClauseValues != null && !inClauseValues.isEmpty()) {
            whereConditions.add(makeInClause(parentJoinField, inClauseValues, preparedStatementParameters));
        }
        if (sortField != null) {
            // Sort field is not provided by user input, so it's ok to add here (i.e., it's not prone to SQL injection).
            sortBy = String.format(" order by %s", sortField);
        }
        Set<String> argumentKeys = graphQLQueryArguments.keySet();
        for (String key : argumentKeys) {
            // The pagination, bounding box, and date/time args should all be skipped here because they are handled
            // separately below from standard args (pagination becomes limit/offset clauses, bounding box applies to
            // stops table, and date/time args filter stop times. All other args become "where X in A, B, C" clauses.
            if (argsToSkip.contains(key)) continue;
            if (ID_ARG.equals(key)) {
                Integer value = (Integer) graphQLQueryArguments.get(key);
                whereConditions.add(String.join(" = ", "id", value.toString()));
            } else {
                List<String> values = (List<String>) graphQLQueryArguments.get(key);
                if (values != null && !values.isEmpty()) whereConditions.add(makeInClause(key, values, preparedStatementParameters));
            }
        }
        if (argumentKeys.containsAll(boundingBoxArgs)) {
            Set<String> boundsConditions = new HashSet<>();
            // Handle bounding box arguments if ALL are supplied. The stops falling within the bounds will be returned.
            // If operating on the stops table, this will just add the bounds filters to the conditions list. If
            // operating on the patterns table, a SELECT DISTINCT patterns query will be constructed with a join to
            // stops and pattern stops.
            for (String bound : boundingBoxArgs) {
                Double value = (Double) graphQLQueryArguments.get(bound);
                // Determine delimiter/equality operator based on min/max
                String delimiter = bound.startsWith("max") ? " <= " : " >= ";
                // Determine field based on lat/lon
                String field = bound.toLowerCase().endsWith("lon") ? "stop_lon" : "stop_lat";
                // Scope field with namespace and table name
                String fieldWithNamespace = String.join(".", namespace, "stops", field);
                boundsConditions.add(String.join(delimiter, fieldWithNamespace, value.toString()));
            }
            if ("stops".equals(tableName)) {
                whereConditions.addAll(boundsConditions);
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
                whereConditions.add(String.format("%s.patterns.pattern_id = unique_pattern_ids_in_bounds.pattern_id", namespace));
            }
        }
        if (argumentKeys.contains(DATE_ARG)) {
            // Handle filtering stop_times by date and period of time (e.g., 4/12/2018 8:00am-11:00am). NOTE: time args
            // are actually specified in seconds since midnight.
            // NOTE: This query must be run only on feeds that have run through validation, which generates the
            // service_dates table. In other words, feeds generated by the editor cannot be queried with the date/time args.
            String tripsTable = String.format("%s.trips", namespace);
            fromTables.add(tripsTable);
            String date = getDateArgument(graphQLQueryArguments);
            // Gather all service IDs that run on the provided date.
            fromTables.add(String.format(
                    "(select distinct service_id from %s.service_dates where service_date = ?) as unique_service_ids_in_operation",
                    namespace)
            );
            // Add date to beginning of preparedStatementParameters list (it is used to pre-select a table in the from clause before any
            // other conditions or preparedStatementParameters are appended).
            preparedStatementParameters.add(0, date);
            if (argumentKeys.contains(FROM_ARG) && argumentKeys.contains(TO_ARG)) {
                // Determine which trips start in the specified time window by joining to filtered stop times.
                String timeFilteredTrips = "trips_beginning_in_time_period";
                whereConditions.add(String.format("%s.trip_id = %s.trip_id", timeFilteredTrips, tripsTable));
                // Select all trip IDs that start during the specified time window. Note: the departure and arrival times
                // are divided by 86399 to account for trips that begin after midnight. FIXME: Should this be 86400?
                fromTables.add(String.format(
                        "(select trip_id " +
                        "from (select distinct on (trip_id) * from %s.stop_times order by trip_id, stop_sequence) as first_stop_times " +
                        "where departure_time %% 86399 >= %d and departure_time %% 86399 <= %d) as %s",
                        namespace,
                        (int) graphQLQueryArguments.get(FROM_ARG),
                        (int) graphQLQueryArguments.get(TO_ARG),
                        timeFilteredTrips));
            }
            // Join trips to service_dates (unique_service_ids_in_operation).
            whereConditions.add(String.format("%s.service_id = unique_service_ids_in_operation.service_id", tripsTable));
        }
        if (argumentKeys.contains(SEARCH_ARG)) {
            // Handle string search argument
            String value = (String) graphQLQueryArguments.get(SEARCH_ARG);
            if (!value.isEmpty()) {
                // Only apply string search if string is not empty.
                Set<String> searchFields = getSearchFields(namespace);
                List<String> searchClauses = new ArrayList<>();
                for (String field : searchFields) {
                    // Double percent signs format as single percents, which are used for the string matching.
                    // FIXME: is ILIKE compatible with non-Postgres? LIKE doesn't work well enough (even when setting
                    // the strings to lower case).
                    searchClauses.add(String.format("%s ILIKE ?", field));
                    preparedStatementParameters.add(String.format("%%%s%%", value));
                }
                if (!searchClauses.isEmpty()) {
                    // Wrap string search in parentheses to isolate from other conditions.
                    whereConditions.add(String.format(("(%s)"), String.join(" OR ", searchClauses)));
                }
            }
        }
        sqlStatementStringBuilder.append(String.format(" from %s", String.join(", ", fromTables)));
        if (!whereConditions.isEmpty()) {
            sqlStatementStringBuilder.append(" where ");
            sqlStatementStringBuilder.append(String.join(" and ", whereConditions));
        }
        // The default value for sortBy is an empty string, so it's safe to always append it here. Also, there is no
        // threat of SQL injection because the sort field value is not user input.
        sqlStatementStringBuilder.append(sortBy);
        Integer limit = (Integer) graphQLQueryArguments.get(LIMIT_ARG);
        if (limit == null) {
            limit = autoLimit ? DEFAULT_ROWS_TO_FETCH : -1;
        }
        if (limit > MAX_ROWS_TO_FETCH) {
            limit = MAX_ROWS_TO_FETCH;
        }
        if (limit == -1) {
            // Do not append limit if explicitly set to -1 or autoLimit is disabled. NOTE: this conditional block is
            // empty simply because it is clearer to define the condition in this way (vs. if limit > 0).
            // FIXME: Skipping limit is not scalable in many cases and should possibly be removed/limited.
        } else {
            sqlStatementStringBuilder.append(" limit ").append(limit);
        }
        Integer offset = (Integer) graphQLQueryArguments.get(OFFSET_ARG);
        if (offset != null && offset >= 0) {
            sqlStatementStringBuilder.append(" offset ").append(offset);
        }
        return jdbcDataLoader.load(new JdbcQuery(
            namespace,
            preparedStatementParameters,
            sqlStatementStringBuilder.toString()
        ));
    }

    private static String getDateArgument(Map<String, Object> arguments) {
        String date = (String) arguments.get(DATE_ARG);
        if (date == null || date.length() != 8) {
            throw new IllegalArgumentException("Date argument must be a string in the format 'YYYYMMDD'.");
        }
        try {
            Integer.parseInt(date);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Date argument must be a string in the format 'YYYYMMDD'.");
        }
        return date;
    }

    /**
     * Checks that the namespace argument is clean and meets the SQL schema specifications.
     * @param namespace database schema namespace/table prefix
     * @return
     */
    public static void validateNamespace(String namespace) {
        if (namespace == null) {
            // If namespace is null, do no attempt a query on a namespace that does not exist.
            throw new IllegalArgumentException("Namespace prefix must be provided.");
        }
        if (!namespace.matches("^[a-zA-Z0-9_]*$")) {
            // Prevent any special characters from being passed into namespace argument.
            throw new IllegalArgumentException("Namespace prefix must contain only alpha, numeric, and underscores.");
        }
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

    /**
     * Construct filter clause with '=' (single string) and add values to list of preparedStatementParameters.
     * */
    static String filterEquals(String filterField, String string, List<String> parameters) {
        // Add string to list of preparedStatementParameters (to be later used to set preparedStatementParameters for prepared sqlStatement).
        parameters.add(string);
        return String.format("%s = ?", filterField);
    }

    /**
     * Construct filter clause with '=' (single string) or 'in' (multiple strings) and add values to list of preparedStatementParameters.
     * */
    static String makeInClause(String filterField, List<String> strings, List<String> parameters) {
        if (strings.size() == 1) {
            return filterEquals(filterField, strings.get(0), parameters);
        } else {
            // Add strings to list of preparedStatementParameters (to be later used to set preparedStatementParameters for prepared sqlStatement).
            parameters.addAll(strings);
            String questionMarks = String.join(", ", Collections.nCopies(strings.size(), "?"));
            return String.format("%s in (%s)", filterField, questionMarks);
        }
    }

    public static class JdbcQuery {
        public String namespace;
        public List<String> preparedStatementParameters;
        public String sqlStatement;


        public JdbcQuery(String namespace, List<String> preparedStatementParameters, String sqlStatement) {
            this.namespace = namespace;
            this.preparedStatementParameters = preparedStatementParameters;
            this.sqlStatement = sqlStatement;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            JdbcQuery jdbcQuery = (JdbcQuery) o;
            return Objects.equals(namespace, jdbcQuery.namespace) &&
                Objects.equals(preparedStatementParameters, jdbcQuery.preparedStatementParameters) &&
                Objects.equals(sqlStatement, jdbcQuery.sqlStatement);
        }

        @Override
        public int hashCode() {
            return Objects.hash(namespace, preparedStatementParameters, sqlStatement);
        }
    }

}
