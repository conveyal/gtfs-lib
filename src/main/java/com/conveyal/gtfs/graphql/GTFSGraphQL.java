package com.conveyal.gtfs.graphql;

import com.conveyal.gtfs.graphql.fetchers.JDBCFetcher;
import graphql.ExecutionInput;
import graphql.GraphQL;
import graphql.execution.instrumentation.dataloader.DataLoaderDispatcherInstrumentation;
import graphql.schema.DataFetchingEnvironment;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderRegistry;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

import static com.conveyal.gtfs.graphql.fetchers.JDBCFetcher.newJdbcDataLoader;

/**
 * This provides a GraphQL API around the gtfs-lib JDBC storage.
 * This just makes a Java API with the right schema available, which uses String requests and responses.
 * To make this into a web API you need to wrap it in an HTTP framework / server.
 */
public class GTFSGraphQL {
    public static Map<String, Object> run (DataSource dataSource, String query, Map<String,Object> variables) {
        // the registry and dataLoaders must be created upon each request to avoid caching loaded data after each request
        DataLoaderRegistry registry = new DataLoaderRegistry();
        DataLoader<JDBCFetcher.JdbcQuery, List<Map<String, Object>>> jdbcQueryDataLoader = newJdbcDataLoader();
        registry.register("jdbcfetcher", jdbcQueryDataLoader);

        return GraphQL.newGraphQL(GraphQLGtfsSchema.feedBasedSchema)
            // this instrumentation implementation will dispatch all the dataloaders
            // as each level fo the graphql query is executed and hence make batched objects
            // available to the query and the associated DataFetchers
            .instrumentation(new DataLoaderDispatcherInstrumentation(registry))
            .build()
            // we now have a graphQL schema with dataloaders instrumented.  Time to make a query
            .execute(
                ExecutionInput.newExecutionInput()
                    .context(new GraphQLExecutionContext(jdbcQueryDataLoader, dataSource))
                    .query(query)
                    .variables(variables)
                    .build()
            )
            .toSpecification();
    }

    public static DataLoader<JDBCFetcher.JdbcQuery, List<Map<String, Object>>> getJdbcQueryDataLoaderFromContext (
        DataFetchingEnvironment environment
    ) {
        return ((GraphQLExecutionContext)environment.getContext()).jdbcQueryDataLoader;
    }

    public static DataSource getDataSourceFromContext (DataFetchingEnvironment environment) {
        DataSource dataSource = ((GraphQLExecutionContext)environment.getContext()).dataSource;
        if (dataSource == null) {
            throw new RuntimeException("DataSource is not defined, unable to make sql query!");
        }
        return dataSource;
    }

    private static class GraphQLExecutionContext {
        private final DataLoader<JDBCFetcher.JdbcQuery, List<Map<String, Object>>> jdbcQueryDataLoader;
        private final DataSource dataSource;

        private GraphQLExecutionContext(DataLoader<JDBCFetcher.JdbcQuery, List<Map<String, Object>>> jdbcQueryDataLoader, DataSource dataSource) {
            this.jdbcQueryDataLoader = jdbcQueryDataLoader;
            this.dataSource = dataSource;
        }
    }
}
