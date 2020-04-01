package com.conveyal.gtfs.error;

import java.io.Serializable;

/** Indicates that the length of a field is invalid. */
public class FieldLengthError extends GTFSError implements Serializable {
    public static final long serialVersionUID = 1L;

    public FieldLengthError(String file, long line, String field) {
        super(file, line, field);
    }

    @Override public String getMessage() {
        return String.format("Field length is invalid.");
    }

}
