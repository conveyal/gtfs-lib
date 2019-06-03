package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.FareAttribute;

/**
 * Validator for fares that currently just checks that the transfers and transfer_duration fields are harmonious.
 */
public class FaresValidator extends FeedValidator {
    public FaresValidator(Feed feed, SQLErrorStorage errorStorage) {
        super(feed, errorStorage);
    }

    @Override
    public void validate() {
        for (FareAttribute fareAttribute : feed.fareAttributes) {
            if (fareAttribute.transfers == 0 && fareAttribute.transfer_duration > 0) {
                // If a fare does not permit transfers, but defines a duration for which a transfer is valid, register
                // an error.
                registerError(fareAttribute, NewGTFSErrorType.FARE_TRANSFER_MISMATCH);
            }
        }
    }
}
