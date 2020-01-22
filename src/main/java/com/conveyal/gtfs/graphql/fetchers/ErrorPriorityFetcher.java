package com.conveyal.gtfs.graphql.fetchers;

import com.conveyal.gtfs.error.NewGTFSErrorType;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLFieldDefinition;

import java.util.Map;

import static graphql.Scalars.GraphQLString;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;

/**
 * A fetcher that is specifically used for looking up and returning the priority of a specific error_type
 */
public class ErrorPriorityFetcher implements DataFetcher {
    public static GraphQLFieldDefinition field(String name) {
        return newFieldDefinition()
            .name(name)
            .type(GraphQLString)
            .dataFetcher(new ErrorPriorityFetcher())
            .build();
    }

    @Override
    public Object get(DataFetchingEnvironment dataFetchingEnvironment) {
        Object source = dataFetchingEnvironment.getSource();
        String errorType = (String) ((Map<String, Object>)source).get("error_type");
        return NewGTFSErrorType.valueOf(errorType).priority;
    }
}
