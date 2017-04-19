package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.loader.Feed;

/**
 * TODO build histogram of stop times, check against calendar and declared feed validity dates
 *
 * Created by landon on 5/26/16.
 */
public class DatesValidator extends FeedValidator {
    @Override
    public boolean validate(Feed feed, boolean repair) {
        throw new UnsupportedOperationException();
    }
}
