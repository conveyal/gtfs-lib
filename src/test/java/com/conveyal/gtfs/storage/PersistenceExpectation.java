package com.conveyal.gtfs.storage;

import com.conveyal.gtfs.GTFSTest;

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


    public PersistenceExpectation(String tableName, RecordExpectation[] recordExpectations) {
        this.tableName = tableName;
        this.recordExpectations = recordExpectations;
    }

    public static PersistenceExpectation[] list (PersistenceExpectation... expectations) {
        return expectations;
    }
}
