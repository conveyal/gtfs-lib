package com.conveyal.gtfs.error;

import com.conveyal.gtfs.model.Entity;

import java.util.ArrayList;
import java.util.List;

/**
 * There's a good reason not to use a class hierarchy for errors: we need to instantiate errors from a database.
 * We don't want to use reflection to call different constructors based on the error-type field in the database.
 * We instead use an enum with final fields for severity and affected entity type.
 */
public class NewGTFSError {

    // The type of error encountered
    NewGTFSErrorType type;

    // The incorrect value(s) that caused the error.
    // This is to be displayed as preview information to end users.
    // It contains a semicolon-separated list of key-value pairs separated by equals signs.
    // This is simulating a variable-width key-value column to make this compatible with all SQL backends.
    // TODO maybe model this as a List<KeyValuePair> and leave storage to another layer
    String badValues;

    List<EntityReference> referencedEntities;

    // TODO methods to add badValues key-value pairs, and to add entities to an existing error.

    public NewGTFSError(NewGTFSErrorType type, String badValues, Entity... entities) {
        this.type = type;
        this.badValues = badValues;
        this.referencedEntities = new ArrayList<>();
        for (Entity entity : entities) {
            this.referencedEntities.add(new EntityReference(entity));
        }
    }

    public static class EntityReference {
        Class<? extends Entity> type;
        // 31 bits is enough, unlikely we'll see files with over 2 billion lines (3 orders of magnitude greater than NL)
        int lineNumber;
        String id;
        Integer sequenceNumber; // Use Integer object because this is often missing (null)
        public EntityReference (Entity entity) {
            type = entity.getClass();
            id = entity.getId();
            sequenceNumber = entity.getSequenceNumber();
            lineNumber = (int) entity.sourceFileLine;
        }
    }

}
