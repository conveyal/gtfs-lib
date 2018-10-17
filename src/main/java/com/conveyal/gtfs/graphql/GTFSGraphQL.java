package com.conveyal.gtfs.graphql;

import com.conveyal.gtfs.graphql.fetchers.JDBCFetcher;
import graphql.ExecutionInput;
import graphql.GraphQL;
import graphql.execution.instrumentation.dataloader.DataLoaderDispatcherInstrumentation;
import graphql.introspection.IntrospectionQuery;
import graphql.schema.DataFetchingEnvironment;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderRegistry;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

import static com.conveyal.gtfs.graphql.fetchers.JDBCFetcher.newJdbcDataLoader;

public class GTFSGraphQL {

    /**
     * Execute a graphQL request.  This method creates and executes a new graphQL runner that is able to cache and batch
     * sql queries during the course of obtaining the data according to the graphQL query.  The resulting output is a
     * map of objects.  This map is typically converted to JSON or some other kind of output via a downstream
     * application using this method.
     *
     * A DataLoader (https://github.com/graphql-java/java-dataloader) handles The batching and caching of requests.
     * java-dataloader requires manual dispatching in order to actually execute the code to fetch data.  However, there
     * is a helper class called DataLoaderDispatcherInstrumentation that automatically dispatches dataloaders.  This was
     * setup according to this guide: https://www.graphql-java.com/documentation/v10/batching/
     *
     * @param dataSource The dataSource to use in connecting to a database to make sql queries.
     * @param query The graphQL query
     * @param variables The variables to apply to the graphQL query
     */
    public static Map<String, Object> run (DataSource dataSource, String query, Map<String,Object> variables) {
        // the registry and dataLoaders must be created upon each request to avoid caching loaded data after each request
        DataLoaderRegistry registry = new DataLoaderRegistry();
        DataLoader<JDBCFetcher.JdbcQuery, List<Map<String, Object>>> jdbcQueryDataLoader = newJdbcDataLoader();
        registry.register("jdbcfetcher", jdbcQueryDataLoader);

        return GraphQL.newGraphQL(GraphQLGtfsSchema.feedBasedSchema)
            // This instrumentation implementation will dispatch all the dataloaders as each level of the graphql query
            // is executed and hence make batched objects available to the query and the associated DataFetchers.  If
            // the dataloader was not recreated on each request and reused between different graphql queries, the
            // dataloader could end up returning cached data for the result of a sql query even if the actual data in
            // the database had changed.  Therefore, the dataloader and registry must be created new each time to avoid
            // caching sql query results in future requests.
            .instrumentation(new DataLoaderDispatcherInstrumentation(registry))
            .build()
            // we now have a graphQL schema with dataloaders instrumented.  Time to make a query
            .execute(
                // Build the execution input giving an environment context, query and variables.  The environment
                // context is used primarily in the JDBCFetcher to get the current loader and dataSource from which
                // the sql queries can be executed.
                ExecutionInput.newExecutionInput()
                    .context(new GraphQLExecutionContext(jdbcQueryDataLoader, dataSource))
                    .query(query)
                    .variables(variables)
                    .build()
            )
            .toSpecification();
    }

    public static final Map<String, Object> introspectedSchema = GraphQL.newGraphQL(GraphQLGtfsSchema.feedBasedSchema)
        .build()
        .execute(IntrospectionQuery.INTROSPECTION_QUERY)
        .toSpecification();

    /**
     * Helper to obtain the jdbcQueryLoader.  It is critical that the same dataloader in the DataloaderRegistry /
     * DataLoaderDispatcherInstrumentation be used otherwise the queries will never be able to be dispatched.
     */
    public static DataLoader<JDBCFetcher.JdbcQuery, List<Map<String, Object>>> getJdbcQueryDataLoaderFromContext (
        DataFetchingEnvironment environment
    ) {
        return ((GraphQLExecutionContext)environment.getContext()).jdbcQueryDataLoader;
    }

    /**
     * Helper method to extract the DataSource from the DataFetchingEnvironment.  Whenever we a sql query needs to be
     * made, this method will have to be called to make sure the same dataSource is being used within the same graphQL
     * query.
     */
    public static DataSource getDataSourceFromContext (DataFetchingEnvironment environment) {
        DataSource dataSource = ((GraphQLExecutionContext)environment.getContext()).dataSource;
        if (dataSource == null) {
            throw new RuntimeException("DataSource is not defined, unable to make sql query!");
        }
        return dataSource;
    }

    /**
     * The object used as the DataFetchingEnvironment context in graphQL queries.
     */
    private static class GraphQLExecutionContext {
        private final DataLoader<JDBCFetcher.JdbcQuery, List<Map<String, Object>>> jdbcQueryDataLoader;
        private final DataSource dataSource;

        private GraphQLExecutionContext(DataLoader<JDBCFetcher.JdbcQuery, List<Map<String, Object>>> jdbcQueryDataLoader, DataSource dataSource) {
            this.jdbcQueryDataLoader = jdbcQueryDataLoader;
            this.dataSource = dataSource;
        }
    }
}
