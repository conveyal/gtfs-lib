package com.conveyal.gtfs.storage;

/**
 * Created by abyrd on 2017-03-25
 */
public class StorageException extends RuntimeException {

    public StorageException(Exception ex) {
        super(ex);
    }

    public StorageException (String message) {
        super(message);
    }

}
