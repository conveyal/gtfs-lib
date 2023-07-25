package com.conveyal.gtfs.error;

import com.conveyal.gtfs.validator.model.Priority;

/**
 * Interface the enum implements, allowing for externally defined error types
 */
public interface NewGTFSErrorInterface {
    Priority priority = null;
    String englishMessage = null;

    String name();
}
