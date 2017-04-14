package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.GTFSError;
import com.conveyal.gtfs.error.GeneralError;
import com.conveyal.gtfs.loader.Feed;

import java.util.ArrayList;
import java.util.List;

/**
 * A Validator examines a GTFS feed in the form of a set of maps, one map per table in the GTFS feed.
 * It adds error messages to that feed, optionally repairing the problems it encounters.
 */
public abstract class Validator {

    protected List<GTFSError> errors = new ArrayList<>();

    /**
     * The main extension point.
     * TODO return errors themselves rather than a boolean?
     * @param feed the feed to validate and optionally repair
     * @param repair if this is true, repair any errors encountered
     * @return whether any errors were encountered
     */
    public abstract boolean validate (Feed feed, boolean repair);

    protected void registerError(String message) {
        errors.add(new GeneralError("file", 0, "field", message));
    }

    public int getErrorCount() {
        return errors.size();
    }

    public boolean foundErrors() {return errors.size() > 0; }
}
