package com.conveyal.gtfs.graphql.fetchers;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLOutputType;

import java.util.Map;

import static graphql.Scalars.GraphQLString;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;

/**
 * This just grabs an entry out of a Map<String, Object>
 * It allows pulling a single field out of the result of a DataFetcher that always returns all fields
 * (like our JDBC SQL fetcher).
 */
public class MapFetcher implements DataFetcher {

    final String key;

    public MapFetcher(String key) {
        this.key = key;
    }

    @Override
    public Object get(DataFetchingEnvironment dataFetchingEnvironment) {
        Object source = dataFetchingEnvironment.getSource();
        return ((Map<String, Object>)source).get(key);
    }

    public static GraphQLFieldDefinition field(String name) {
        return field(name, GraphQLString);
    }

    public static GraphQLFieldDefinition field(String name, GraphQLOutputType type) {
        return newFieldDefinition()
                .name(name)
                .type(type)
                .dataFetcher(new MapFetcher(name))
                .build();
    }

}
