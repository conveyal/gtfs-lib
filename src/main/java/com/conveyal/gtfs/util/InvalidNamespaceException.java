package com.conveyal.gtfs.util;

public class InvalidNamespaceException extends Exception {
    public InvalidNamespaceException() {
        super("Namespace must only have alphanumeric characters or the underscore symbol");
    }
}
