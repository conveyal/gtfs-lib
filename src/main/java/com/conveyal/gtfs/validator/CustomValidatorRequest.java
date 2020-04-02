package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;

/**
 * An interface with a callback to instantiate a custom GTFS Feed validator
 * using the Feed and SQLErrorStorage objects.
 */
public interface CustomValidatorRequest {
    /**
     * The callback that instantiates and returns instances of custom FeedValidator objects
     * constructed using the provided feed and error storage objects.
     * @param feed The feed being validated.
     * @param errorStorage The object that handles error storage.
     */
    FeedValidator apply(Feed feed, SQLErrorStorage errorStorage);
}
