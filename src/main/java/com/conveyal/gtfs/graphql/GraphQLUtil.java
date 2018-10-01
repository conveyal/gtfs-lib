package com.conveyal.gtfs.graphql;

import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import graphql.schema.PropertyDataFetcher;

import static graphql.Scalars.GraphQLFloat;
import static graphql.Scalars.GraphQLInt;
import static graphql.Scalars.GraphQLString;
import static graphql.schema.GraphQLArgument.newArgument;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;

public class GraphQLUtil {

    public static GraphQLFieldDefinition string (String name) {
        return newFieldDefinition()
                .name(name)
                .type(GraphQLString)
                .dataFetcher(new PropertyDataFetcher(name))
                .build();
    }

    public static GraphQLFieldDefinition intt (String name) {
        return newFieldDefinition()
                .name(name)
                .type(GraphQLInt)
                .dataFetcher(new PropertyDataFetcher(name))
                .build();
    }

    public static GraphQLArgument stringArg (String name) {
        return newArgument()
                .name(name)
                .type(GraphQLString)
                .build();
    }

    public static GraphQLArgument multiStringArg (String name) {
        return newArgument()
                .name(name)
                .type(new GraphQLList(GraphQLString))
                .build();
    }

    public static GraphQLArgument intArg (String name) {
        return newArgument()
                .name(name)
                .type(GraphQLInt)
                .build();
    }

    public static GraphQLArgument floatArg (String name) {
        return newArgument()
                .name(name)
                .type(GraphQLFloat)
                .build();
    }

    /**
     * Helper method to get the namespaced table name
     */
    public static String namespacedTableName(String namespace, String tableName) {
        return String.format("%s.%s", namespace, tableName);
    }

    /**
     * Helper method to get the string of the column name with the namespace and table name
     */
    public static String namespacedTableFieldName(String namespace, String tableName, String tableField) {
        return String.format("%s.%s.%s", namespace, tableName, tableField);
    }
}
