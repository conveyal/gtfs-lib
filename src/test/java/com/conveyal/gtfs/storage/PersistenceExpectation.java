package com.conveyal.gtfs.storage;

import java.util.Arrays;

/**
 * A helper class to verify that data got stored in a particular table.
 */
public class PersistenceExpectation {
    public String tableName;
    /**
     * Each persistence expectation has an array of record expectations which all must reference a single row.
     * If looking for multiple records in the same table, create numerous PersistenceExpectations with the same
     * tableName.
     */
    public RecordExpectation[] recordExpectations;
    public boolean appliesToEditorDatabaseOnly;


    public PersistenceExpectation(String tableName, RecordExpectation[] recordExpectations) {
        this(tableName, recordExpectations, false);
    }

    public PersistenceExpectation(
        String tableName,
        RecordExpectation[] recordExpectations,
        boolean appliesToEditorDatabaseOnly
    ) {
        this.tableName = tableName;
        this.recordExpectations = recordExpectations;
        this.appliesToEditorDatabaseOnly = appliesToEditorDatabaseOnly;
    }

    public static PersistenceExpectation[] list (PersistenceExpectation... expectations) {
        return expectations;
    }

    @Override
    public String toString() {
        return "PersistenceExpectation{" + "tableName='" + tableName + '\'' + ", recordExpectations=" + Arrays
            .toString(recordExpectations) + ", appliesToEditorDatabaseOnly=" + appliesToEditorDatabaseOnly + '}';
    }
}
