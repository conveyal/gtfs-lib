package com.conveyal.gtfs.graphql.fetchers;

import com.conveyal.gtfs.graphql.GraphQLGtfsSchema;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import org.apache.commons.dbutils.DbUtils;
import org.dataloader.BatchLoaderEnvironment;
import org.dataloader.DataLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.conveyal.gtfs.graphql.GTFSGraphQL.getDataSourceFromContext;
import static com.conveyal.gtfs.graphql.GTFSGraphQL.getJdbcQueryDataLoaderFromContext;
import static com.conveyal.gtfs.graphql.GraphQLUtil.multiStringArg;
import static com.conveyal.gtfs.graphql.GraphQLUtil.stringArg;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;

/**
 * A generic fetcher to get fields out of an SQL database table.
 */
public class JDBCFetcher implements DataFetcher<CompletableFuture<List<Map<String, Object>>>> {

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

    /**
     * Get a new MappedBatchLoaderWithContext batch loader that is able to batch multiple queries and cache the results.
     */
    public static DataLoader<JdbcQuery, List<Map<String, Object>>> newJdbcDataLoader() {
        return DataLoader.newMappedDataLoader(
            (jdbcQueries, context) -> CompletableFuture.supplyAsync(() -> getSqlQueryResults(jdbcQueries, context))
        );
    }

    /**
     * Method where batched sql queries are to be run.  This method first iterates through all queries to see if there
     * are ran any possibilities for combining multiple similar queries into one query where the results can be
     * extracted and returned to the appropriate jdbcQuery.
     *
     * For now, only certain kinds of queries can be combined.  See the CombinedQuery class for more details.
     */
    private static Map<JdbcQuery, List<Map<String, Object>>> getSqlQueryResults(
        Set<JdbcQuery> jdbcQueries,
        BatchLoaderEnvironment context
    ) {
        // create output map for all results
        Map<JdbcQuery, List<Map<String, Object>>> allResults = new HashMap<>();

        // get all key contexts
        Map<Object, Object> keyContexts = context.getKeyContexts();

        // prepare a list of sql queries that will actually get executed
        List<CombinedQuery> combinedQueries = new ArrayList<>();

        // iterate through all batched jdbcQueries and combine combineable queries
        for (JdbcQuery jdbcQuery : jdbcQueries) {
            // get the dataSource from the context of this specific jdbcQuery
            DataSource dataSource = ((DataSource) keyContexts.get(jdbcQuery));

            // add first query to list
            if (combinedQueries.isEmpty()) {
                combinedQueries.add(new CombinedQuery(jdbcQuery, dataSource));
                continue;
            }

            // check if subsequent queries can be merged with other queries
            boolean foundOtherQueryToCombineWith = false;
            for (CombinedQuery combinedQuery : combinedQueries) {
                if (combinedQuery.combineWith(jdbcQuery)) {
                    foundOtherQueryToCombineWith = true;
                    break;
                }
            }

            // wasn't able to merge with another query, so add as another query to execute
            if (!foundOtherQueryToCombineWith) {
                combinedQueries.add(new CombinedQuery(jdbcQuery, dataSource));
            }
        }

        // execute each combineable query and add to the results
        for (CombinedQuery combinedQuery : combinedQueries) {
            combinedQuery.executeAndAddToResults(allResults);
        }

        return allResults;
    }

    /**
     * Helper method to actually execute some sql.
     *
     * @param connection The database connection to use
     * @param namespace The database namespace that will be included in the output
     * @param statement The sql statement to be executed
     * @param preparedStatementParameters A list of parameters to apply to the prepared statement.  Assumes that all
     *                                    parameters are strings.
     * @throws SQLException
     */
    public static List<Map<String, Object>> getSqlQueryResult (
        Connection connection,
        String namespace,
        String statement,
        List<String> preparedStatementParameters
    ) throws SQLException {
        List<Map<String, Object>> results = new ArrayList<>();
        PreparedStatement preparedStatement = connection.prepareStatement(statement);
        int oneBasedIndex = 1;
        for (String parameter : preparedStatementParameters) {
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
                resultMap.put("namespace", namespace);
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
    }

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

    /**
     * get some data by fetching data from the database.
     */
    @Override
    public CompletableFuture<List<Map<String, Object>>> get (DataFetchingEnvironment environment) {
        // If we are fetching an item nested within a GTFS entity in the GraphQL query, we want to add an SQL "where"
        // clause using the values found here. Note, these are used in the below getResults call.
        List<String> inClauseValues = new ArrayList<>();
        List<String> whereConditions = new ArrayList<>();
        List<String> preparedStatementParameters = new ArrayList<>();
        if (parentJoinField != null) {
            Map<String, Object> enclosingEntity = environment.getSource();
            // FIXME: THIS IS BROKEN if parentJoinValue is null!!!!
            Object inClauseValue = enclosingEntity.get(parentJoinField);
            // Check for null parentJoinValue to protect against NPE.
            String inClauseString = inClauseValue == null ? null : inClauseValue.toString();
            inClauseValues.add(inClauseString);
            if (inClauseValue == null) {
                return new CompletableFuture<>();
            }
            whereConditions.add(makeInClause(parentJoinField, inClauseValues, preparedStatementParameters));
        }

        return getResults(
            environment,
            // We could select only the requested fields by examining environment.getFields(), but we just get them all.
            // The advantage of selecting * is that we don't need to validate the field names.
            // All the columns will be loaded into the Map<String, Object>,
            // but only the requested fields will be fetched from that Map using a MapFetcher.
            "select *",
            new HashSet<>(), // No additional tables are needed beyond those added by default in the next method.
            whereConditions,
            preparedStatementParameters
        );
    }

    /**
     * Handle fetching functionality for a given namespace, set of join values, and arguments. This is broken out from
     * the standard get function so that it can be reused in other fetchers (i.e., NestedJdbcFetcher).  This method
     * ultimately returns a CompletableFuture result by calling the load method on the jdbcDataLoader argument.
     * Therefore, this can't be directly used to get the results of a sql query.  Instead it is intended to be used only
     * by DataFetchers that get their data by making a sql query and get their results in the format of
     * List<Map<String, Object>>.
     *
     * @param environment The datafetching environement of the current GraphQL entity being fetched.
     * @param selectStatement The beginnings of the sql statement string.  The NestedJdbcFetcher must select only from
     *                        the final table, so this argument is needed.
     * @param fromTables The tables to select from.
     * @param whereConditions A list of where conditions to apply to the query.
     * @param preparedStatementParameters A list of prepared statement parameters to be sent to the prepared statement
     *                                    once the query is ran.
     */
    CompletableFuture<List<Map<String, Object>>> getResults (
        DataFetchingEnvironment environment,
        String selectStatement,
        Set<String> fromTables,
        List<String> whereConditions,
        List<String> preparedStatementParameters
    ) {
        // GetSource is the context in which this this DataFetcher has been created, in this case a map representing
        // the parent feed (FeedFetcher).
        Map<String, Object> parentEntityMap = environment.getSource();

        // This DataFetcher only makes sense when the enclosing parent object is a feed or something in a feed.
        // So it should always be represented as a map with a namespace key.
        String namespace = (String) parentEntityMap.get("namespace");

        // these can be additional arguments to apply to the query such as a limit on the amount of results
        Map<String, Object> graphQLQueryArguments = environment.getArguments();

        // Use the datasource defined in theDataFetching environment to query the database.
        DataSource dataSource = getDataSourceFromContext(environment);

        // This will contain one Map<String, Object> for each row fetched from the database table.
        List<Map<String, Object>> results = new ArrayList<>();
        if (graphQLQueryArguments == null) graphQLQueryArguments = new HashMap<>();
        // Ensure namespace exists and is clean. Note: FeedFetcher will have executed before this and validated that an
        // entry exists in the feeds table and the schema actually exists in the database.
        validateNamespace(namespace);

        // The primary table being selected from is added here. Other tables may have been / will be added to this list to handle joins.
        fromTables.add(String.join(".", namespace, tableName));

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
                Set<String> searchFields = getSearchFields(dataSource, namespace);
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

        // The default value for sortBy is an empty string, so it's safe to always append it here. Also, there is no
        // threat of SQL injection because the sort field value is not user input.
        String sortBy = "";
        if (sortField != null) {
            // Sort field is not provided by user input, so it's ok to add here (i.e., it's not prone to SQL injection).
            sortBy = String.format(" order by %s", sortField);
        }
        Integer limit = (Integer) graphQLQueryArguments.get(LIMIT_ARG);
        if (limit == null) {
            limit = autoLimit ? DEFAULT_ROWS_TO_FETCH : -1;
        }
        if (limit > MAX_ROWS_TO_FETCH) {
            limit = MAX_ROWS_TO_FETCH;
        }
        Integer offset = (Integer) graphQLQueryArguments.get(OFFSET_ARG);

        // Make sure to get the dataloader defined in the environment to avoid caching sql results in future requests.
        DataLoader<JdbcQuery, List<Map<String, Object>>> jdbcDataLoader = getJdbcQueryDataLoaderFromContext(environment);

        // add a new JdbcQuery to the DataLoader.  The DataLoader returns a completeable future and will obtain the
        // result of the query either from a cache or by executing some sql.
        return jdbcDataLoader.load(
            new JdbcQuery(
                namespace,
                selectStatement,
                fromTables,
                whereConditions,
                sortBy,
                limit,
                offset,
                preparedStatementParameters
            ),
            dataSource
        );
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
    private Set<String> getSearchFields(DataSource dataSource, String namespace) {
        // Check table metadata for presence of columns.
//        long startTime = System.currentTimeMillis();
        Set<String> columnsForTable = new HashSet<>();
        List<String> searchColumns = new ArrayList<>();
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
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

    /**
     * Helper class for batching sql queries to the dataloader.  The dataloader is sent instances of this class which it
     * will use to create queries.  We need to override the equals and hashcode methods to ensure proper equality checks
     * when the dataloader is matching the results of the query to the place where it should return the result as part
     * of the graphql response.  This class also serves as a lookup key for the dataloader cache.
     */
    public static class JdbcQuery {
        private final String namespace;
        private final String selectStatement;
        private final Set<String> fromTables;
        private List<String> whereConditions;
        public String sortBy;
        private final Integer limit;
        private final Integer offset;
        private final List<String> preparedStatementParameters;


        public JdbcQuery(
            String namespace,
            String selectStatement,
            Set<String> fromTables,
            List<String> whereConditions,
            String sortBy,
            Integer limit,
            Integer offset,
            List<String> preparedStatementParameters
        ) {
            this.namespace = namespace;
            this.selectStatement = selectStatement;
            this.fromTables = fromTables;
            this.whereConditions = whereConditions;
            this.sortBy = sortBy;
            this.limit = limit;
            this.offset = offset;
            this.preparedStatementParameters = preparedStatementParameters;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            JdbcQuery jdbcQuery = (JdbcQuery) o;
            return Objects.equals(namespace, jdbcQuery.namespace) &&
                Objects.equals(selectStatement, jdbcQuery.selectStatement) &&
                Objects.equals(fromTables, jdbcQuery.fromTables) &&
                Objects.equals(whereConditions, jdbcQuery.whereConditions) &&
                Objects.equals(sortBy, jdbcQuery.sortBy) &&
                Objects.equals(limit, jdbcQuery.limit) &&
                Objects.equals(offset, jdbcQuery.offset) &&
                Objects.equals(preparedStatementParameters, jdbcQuery.preparedStatementParameters);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                namespace,
                selectStatement,
                fromTables,
                whereConditions,
                sortBy,
                limit,
                offset,
                preparedStatementParameters
            );
        }
    }

    /**
     * A helper class that keeps track of queries that are combined from various individual JdbcQueries.  In a lot of
     * cases there will be numerous JdbcQueries that look very similar yet differ only in that the value that is being
     * selected.  For example, two queries could be
     *   `select id from trip where route_id = 1` and
     *   `select id from trip where route_id = 2`
     * The above 2 queries can be combined into one by using an in clause:
     *   `select id from trip where route_id in (1, 2)`
     */
    private static class CombinedQuery {
        // the field used as the in clause in the combined query
        private String inClauseField;

        // a StringBuilder is used to build up the inCondition
        private StringBuilder inConditionClause = new StringBuilder();

        // whether or not this query can be combined with other queries
        private final boolean isCombinable;

        // whether or not at least 2 queries are being combined
        private boolean isCombination = false;
        private List<String> combinedPreparedStatementParameters = new ArrayList<>();

        // a Map of the prepared statement parameter to the JdbcQuery it came from.  This is used when mapping
        private Map<String, JdbcQuery> combinedQueries = new HashMap<>();

        // a list of where conditions excluding the original where condition with a parameter.  That is instead stored
        // in the inConditionClause.
        private List<String> combinedWhereConditions = new ArrayList<>();

        // Assume that the same DataSource can be used to make a combined query.  This should be OK because GraphQL
        // requests in this library take in a DataSource for each GraphQL request.
        private final DataSource underlyingDataSource;
        private JdbcQuery underlyingQuery;


        /**
         * Instantiate the combined query.  It could be the case that the query is not combinable, so do a few checks
         * and prepare some other variables for later use.
         */
        public CombinedQuery(JdbcQuery jdbcQuery, DataSource dataSource) {
            underlyingDataSource = dataSource;
            underlyingQuery = jdbcQuery;
            isCombinable = underlyingQuery.limit == -1 &&
                underlyingQuery.offset == null &&
                underlyingQuery.preparedStatementParameters.size() == 1 &&
                parseWhereQueries();

            // do some extra prep work if it's possible to combine the underlying query
            if (isCombinable) {
                String preparedStatementParameter = underlyingQuery.preparedStatementParameters.get(0);
                combinedQueries.put(preparedStatementParameter, jdbcQuery);
                combinedPreparedStatementParameters.add(preparedStatementParameter);
            }
        }

        /**
         * Helper function to parse the where conditions.  This method returns true if it finds that only one of
         * the where conditions has a question mark.
         */
        private boolean parseWhereQueries() {
            // FIXME: this won't match a where statement where the question mark is first.
            Pattern parameretizedWherePattern = Pattern.compile("(.*?)\\s*=\\s*\\?");
            boolean foundCombinableWhereClause = false;
            for (String whereCondition : underlyingQuery.whereConditions) {
                Matcher matcher = parameretizedWherePattern.matcher(whereCondition);
                if (matcher.find()) {
                    // if a combinable where clause has already been found, return false because this current
                    // implementation only supports 1 parameratized where clause
                    if (foundCombinableWhereClause) return false;

                    // begin creating the inConditionClause
                    inClauseField = matcher.group(1);
                    inConditionClause.append(inClauseField);
                    inConditionClause.append(" IN (?");

                    foundCombinableWhereClause = true;
                } else {
                    combinedWhereConditions.add(whereCondition);
                }
            }
            // return the result.  If no combinable where clauses were found then this returns false.  If just one is
            // found this returns true.  Otherwise the return in the above for loop returns false.
            return foundCombinableWhereClause;
        }

        /**
         * Combine a given JdbcQueries if possible to this combined query which may already have other queries that have
         * been combined.
         *
         * For 2 JdbcQueries to be combinable, the queries must not meet the following criteria:
         * - be from the same namespace
         * - not have a limit or an offset
         * - have the exact same select string, from tables and ordering
         * - have at least 1 where clause
         * - have only 1 prepared statement parameter
         */
        public boolean combineWith(JdbcQuery other) {
            boolean canCombineQueries = isCombinable &&
                underlyingQuery.namespace.equals(other.namespace) &&
                other.limit == -1 &&
                other.offset == null &&
                underlyingQuery.selectStatement.equals(other.selectStatement) &&
                underlyingQuery.fromTables.equals(other.fromTables) &&
                // technically, the ordering shouldn't change the actual records that are returned, it's just more work
                // to do postprocessing to resort the fields
                underlyingQuery.sortBy.equals(other.sortBy) &&
                // FIXME
                // the where conditions are strings in the form %s = %s, so it's possible that there will be a false
                // negative where one where condition is a = b and the other is b = a.
                underlyingQuery.whereConditions.equals(other.whereConditions) &&
                other.preparedStatementParameters.size() == 1;

            // if it's not possible to combine, return false immediately
            if (!canCombineQueries) return canCombineQueries;

            // queries can be combined, proceed with adding the query
            isCombination = true;
            String otherFirstParameter = other.preparedStatementParameters.get(0);
            combinedQueries.put(otherFirstParameter, other);
            combinedPreparedStatementParameters.add(otherFirstParameter);
            inConditionClause.append(", ?");

            return true;
        }

        /**
         * execute the combined (or underlying query if it wasn't combine-able) and then add the results to the passed
         * map
         */
        public void executeAndAddToResults(Map<JdbcQuery, List<Map<String, Object>>> allResults) {
            // build up the sql statement to execute
            StringBuilder sqlStatement = new StringBuilder();

            // SELECT
            sqlStatement.append(underlyingQuery.selectStatement);
            String inClauseFieldAlias = null;
            if (isCombination) {
                // also add the field that is used in the IN clause.  We need this data to match the row to the original
                // JdbcQuery.  Replace the field's .'s with _ to ensure an exact match in the output
                inClauseFieldAlias = String.format("%s_alias", inClauseField.replace(".", "_"));
                sqlStatement.append(String.format(
                    ", %s as %s",
                    inClauseField,
                    inClauseFieldAlias
                ));
            }

            // FROM
            sqlStatement.append(String.format(" from %s", String.join(", ", underlyingQuery.fromTables)));

            // WHERE
            if (isCombination) {
                sqlStatement.append(" where ");
                // complete the in clause and add it to the where conditions
                inConditionClause.append(")");
                combinedWhereConditions.add(inConditionClause.toString());
                sqlStatement.append(String.join(" and ", combinedWhereConditions));
            } else {
                if (!underlyingQuery.whereConditions.isEmpty()) {
                    sqlStatement
                        .append(" where ")
                        .append(String.join(" and ", underlyingQuery.whereConditions));
                }
            }

            // ORDER BY
            sqlStatement.append(underlyingQuery.sortBy);

            // limit and offset only apply if the combined query is not a combination
            if (!isCombination) {
                if (underlyingQuery.limit == -1) {
                    // Do not append limit if explicitly set to -1 or autoLimit is disabled. NOTE: this conditional block is
                    // empty simply because it is clearer to define the condition in this way (vs. if limit > 0).
                    // FIXME: Skipping limit is not scalable in many cases and should possibly be removed/limited.
                } else {
                    sqlStatement.append(" limit ").append(underlyingQuery.limit);
                }
                if (underlyingQuery.offset != null && underlyingQuery.offset >= 0) {
                    sqlStatement.append(" offset ").append(underlyingQuery.offset);
                }
            }

            // Execute the query
            Connection connection = null;
            try {
                connection = underlyingDataSource.getConnection();
                List<Map<String, Object>> results = getSqlQueryResult(
                    connection,
                    underlyingQuery.namespace,
                    sqlStatement.toString(),
                    isCombination ? combinedPreparedStatementParameters : underlyingQuery.preparedStatementParameters
                );

                if (isCombination) {
                    // iterate throught the results and assign each Map to the corresponding entry in allResults
                    for (Map<String, Object> result : results) {
                        JdbcQuery query = combinedQueries.get(result.get(inClauseFieldAlias));
                        List<Map<String, Object>> queryResults = allResults.containsKey(query)
                            ? allResults.get(query)
                            : new ArrayList<>();

                        // remove the alias field
                        result.remove(inClauseFieldAlias);
                        queryResults.add(result);

                        allResults.put(query, queryResults);
                    }
                } else {
                    // query was not combined, so we can add all of these results with the underlying query as the key
                    allResults.put(underlyingQuery, results);
                }
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            } finally {
                DbUtils.closeQuietly(connection);
            }
        }
    }
}
