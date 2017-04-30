package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;

/**
 * Created by abyrd on 2017-04-19
 */
public abstract class FeedValidator extends Validator {

    public FeedValidator (Feed feed, SQLErrorStorage errorStorage) {
        super(feed, errorStorage);
    }

    /** The main extension point. */
    public abstract void validate ();

}
