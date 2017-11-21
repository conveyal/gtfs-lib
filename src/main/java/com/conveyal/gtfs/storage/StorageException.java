package com.conveyal.gtfs.storage;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import org.apache.http.util.ExceptionUtils;

/**
 * Some errors are detected way down the call stack where we don't have a reference to the errorStorage object.
 * We throw this exception to signal the caller that something went wrong.
 * Also, some exceptions may be unexpected/unhandled, and this serves as a catch-all class for any other problems
 * that may arise when loading/storing a GTFS feed.
 */
public class StorageException extends RuntimeException {

    /** For expected, recognized errors that have a defined enum value. */
    public NewGTFSErrorType errorType = NewGTFSErrorType.OTHER;

    /** This is the string that will make it out to the client, explaining what went wrong. */
    public String badValue = null;

    /** This is the constructor for expected errors that have a defined enum value. */
    public StorageException(NewGTFSErrorType errorType, String badValue) {
        super(errorType.englishMessage);
        this.errorType = errorType;
        this.badValue = badValue;
    }

    /** This is the constructor for wrapping unexpected and unhandled exceptions. */
    public StorageException (Exception ex) {
        super(ex);
        // Expose the exception type and message to the outside world.
        badValue = ex.toString();
    }

}
