package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.Entity;

import java.util.ArrayList;
import java.util.List;

/**
 * A Validator examines a whole GTFS feed or a single trip within a GTFS feed.
 * It accumulates error messages for problems it finds in that feed, optionally repairing the problems it encounters.
 */
public abstract class Validator {

    protected List<NewGTFSError> errors = new ArrayList<>();

    protected void registerError (NewGTFSErrorType errorType, String badValue, Entity... entities)  {
        errors.add(new NewGTFSError(errorType, badValue, entities));
    }

    public int getErrorCount() { return errors.size(); }

    public boolean foundErrors() {return errors.size() > 0; }

    public Iterable<NewGTFSError> getErrors () { return errors; }

}
