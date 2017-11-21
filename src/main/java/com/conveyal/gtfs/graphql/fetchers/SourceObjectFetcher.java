package com.conveyal.gtfs.graphql.fetchers;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

import java.util.Map;

/**
 * This is a special data fetcher for cases where the sub-object context is exactly the same as the parent object.
 * This is useful for grouping fields together. For example, row counts of tables within a GTFS feed are conceptually
 * directly under the feed, but we group them together in a sub-object for clarity.
 */
public class SourceObjectFetcher implements DataFetcher {

    @Override
    public Map<String, Object> get(DataFetchingEnvironment dataFetchingEnvironment) {
        return dataFetchingEnvironment.getSource();
    }

}
