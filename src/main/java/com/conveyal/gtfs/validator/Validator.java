package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.Entity;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * A Validator examines a whole GTFS feed or a single trip within a GTFS feed.
 * It accumulates error messages for problems it finds in that feed, optionally repairing the problems it encounters.
 */
public abstract class Validator {

    Feed feed;

    SQLErrorStorage errorStorage;

    public Validator(Feed feed, SQLErrorStorage errorStorage) {
        this.feed = feed;
        this.errorStorage = errorStorage;
    }

    public void registerError (NewGTFSErrorType errorType, String badValue, Entity... entity) {
        errorStorage.storeError(new NewGTFSError(errorType, badValue, entity));
    }

}
