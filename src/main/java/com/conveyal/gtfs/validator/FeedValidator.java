package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.loader.Feed;

/**
 * Created by abyrd on 2017-04-19
 */
public abstract class FeedValidator extends Validator {

    /**
     * The main extension point.
     * TODO return errors themselves rather than a boolean? Or void return type, and call foundErrors() on validator.
     * TODO remove repair capabilities.
     * @param feed the feed to validate and optionally repair
     * @param repair if this is true, repair any errors encountered
     * @return whether any errors were encountered
     */
    public abstract boolean validate (Feed feed, boolean repair);

}
