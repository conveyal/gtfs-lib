package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;

/**
 * TODO build histogram of stop times, check against calendar and declared feed validity dates
 *
 * Created by landon on 5/26/16.
 */
public class DatesValidator extends FeedValidator {

    public DatesValidator(Feed feed, SQLErrorStorage errorStorage) {
        super(feed, errorStorage);
    }

    @Override
    public void validate() {
        throw new UnsupportedOperationException();
    }

}
