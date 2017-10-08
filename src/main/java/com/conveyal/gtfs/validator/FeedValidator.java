package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;

/**
 * A subtype of validator that can validate the entire feed at once.
 */
public abstract class FeedValidator extends Validator {

    public FeedValidator (Feed feed, SQLErrorStorage errorStorage) {
        super(feed, errorStorage);
    }

    /** The main extension point. Each subsclass must define this method. */
    public abstract void validate ();

}
