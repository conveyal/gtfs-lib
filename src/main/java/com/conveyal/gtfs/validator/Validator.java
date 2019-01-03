package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.Entity;

import java.util.Set;

/**
 * A Validator examines a whole GTFS feed or a single trip within a GTFS feed. It accumulates error messages for
 * problems it finds in that feed, optionally repairing the problems it encounters.
 */
public abstract class Validator {

    Feed feed;

    SQLErrorStorage errorStorage;

    public Validator(Feed feed, SQLErrorStorage errorStorage) {
        this.feed = feed;
        this.errorStorage = errorStorage;
    }

    /**
     * Store an error that affects the entire feed or an entire file. Wraps the underlying error constructor.
     */
//    public void registerError (Class<? extends Entity> entityType, NewGTFSErrorType errorType) {
//        errorStorage.storeError(new NewGTFSError(entityType, errorType));
//    }

    /**
     * Store an error that affects a single line of a single table. Wraps the underlying error factory method.
     */
    public void registerError(Entity entity, NewGTFSErrorType errorType) {
        errorStorage.storeError(NewGTFSError.forEntity(entity, errorType));
    }

    /**
     * Stores a set of errors.
     */
    public void storeErrors(Set<NewGTFSError> errors) {
        errorStorage.storeErrors(errors);
    }

    /**
     * WARNING: this method creates but DOES NOT STORE a new GTFS error. It should only be used in cases where a
     * collection of errors need to be temporarily held before storing in batch (e.g., waiting to store travel time zero
     * errors before it is determined that the entire feed uses travel times rounded to the minute).
     */
    NewGTFSError createUnregisteredError (Entity entity, NewGTFSErrorType errorType) {
        return NewGTFSError.forEntity(entity, errorType);
    }

//    /**
//     * Store an error that affects a single line of a single table. Add a single key-value pair to it. Wraps the
//     * underlying error constructor.
//     */
//    public void registerError(Entity entity, NewGTFSErrorType errorType, String key, String value) {
//        errorStorage.storeError(new NewGTFSError(entity, errorType).addInfo(key, value));
//    }

    /**
     * Store an error that affects a single line of a single table.
     * Add a bad value to it.
     */
    public void registerError(Entity entity, NewGTFSErrorType errorType, Object badValue) {
        errorStorage.storeError(NewGTFSError.forEntity(entity, errorType).setBadValue(badValue.toString()));
    }

    /**
     * Basic storage of user-constructed error.
     */
    public void registerError (NewGTFSError error) {
        errorStorage.storeError(error);
    }

    /**
     * This method will be called after the validation process is complete.
     * This allows the implementation to perform any analysis or checking that uses accumulated information, and
     * provides a path to output that summary information (by saving it in the provided ValidationResult object.
     */
    public void complete (ValidationResult validationResult) {}

}