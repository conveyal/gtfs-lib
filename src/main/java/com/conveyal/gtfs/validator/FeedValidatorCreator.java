package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;

/**
 * A functional interface used to create {@link FeedValidator} instances. This interface supports the ability to pass
 * validators by lambda method references ({@code SomeValidator::new}) to the {@link Feed#validate(FeedValidatorCreator...)}
 * method in order to run special validation checks on specific feeds (e.g., {@link MTCValidator} should be run only on
 * GTFS files loaded for those MTC operators). To instantiate the FeedValidator, simply call the
 * {@link #create(Feed, SQLErrorStorage)} method, passing in the feed and errorStorage arguments. This is in lieu of
 * instantiating the FeedValidator with those arguments in the constructor because these objects are not available before
 * the validate method is invoked.
 */
@FunctionalInterface
public interface FeedValidatorCreator {
    /**
     * The callback that instantiates and returns instances of custom FeedValidator objects
     * constructed using the provided feed and error storage objects.
     * @param feed The feed being validated.
     * @param errorStorage The object that handles error storage.
     */
    FeedValidator create(Feed feed, SQLErrorStorage errorStorage);
}
