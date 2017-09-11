package com.conveyal.gtfs.storage;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;

/**
 * Created by abyrd on 2017-03-25
 */
public class StorageException extends RuntimeException {

    public NewGTFSErrorType errorType = NewGTFSErrorType.OTHER;

    public StorageException(NewGTFSErrorType errorType) {
        this.errorType = errorType;
    }

    public StorageException(Exception ex) {
        super(ex);
    }

    public StorageException (String message) {
        super(message);
    }

    public StorageException(String message, Exception ex) {
        super(message, ex);
    }

}
