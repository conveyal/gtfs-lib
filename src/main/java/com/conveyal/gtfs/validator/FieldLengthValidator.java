package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;

/**
 * Unlike FeedValidators that are run against the entire feed, these validators are run against the stop_times for
 * a specific trip. This is an optimization that allows us to fetch and group those stop_times only once.
 */
public class FieldLengthValidator extends Validator {
    public FieldLengthValidator(Feed feed, SQLErrorStorage errorStorage) {
        super(feed, errorStorage);
    }

    public boolean validateFieldLength(String value, int length) {
        return value != null && value.length() > length;
    }
}
