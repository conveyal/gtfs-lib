package com.conveyal.gtfs.storage;

import com.conveyal.gtfs.error.NewGTFSErrorType;

public class ErrorExpectation {
    public NewGTFSErrorType errorType;
    public String badValue;
    public String entityType;
    public String entityId;

    public ErrorExpectation(NewGTFSErrorType errorType) {
        this.errorType = errorType;
    }

    public ErrorExpectation(NewGTFSErrorType errorType, String entityId) {
        this.errorType = errorType;
        this.entityId = entityId;
    }

    public ErrorExpectation(NewGTFSErrorType errorType, String badValue, String entityType, String entityId) {
        this.errorType = errorType;
        this.badValue = badValue;
        this.entityType = entityType;
        this.entityId = entityId;
    }

    public static ErrorExpectation[] list (ErrorExpectation... expectations) {
        return expectations;
    }
}
