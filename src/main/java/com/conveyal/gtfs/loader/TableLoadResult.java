package com.conveyal.gtfs.loader;

import java.io.Serializable;

/**
 * An instance of this class is returned by the method that loads a single GTFS table.
 * It contains summary information about what happened while loading that one table.
 */
public class TableLoadResult implements Serializable {

    private static final long serialVersionUID = 1L;
    public int rowCount;
    public int errorCount;
    public String fatalException = null;
    public int fileSize;

    /** No-arg constructor for Mongo */
    public TableLoadResult () { }

}
