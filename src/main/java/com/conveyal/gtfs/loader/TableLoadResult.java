package com.conveyal.gtfs.loader;

/**
 * An instance of this class is returned by the method that loads a single GTFS table.
 * It contains summary information about what happened while loading that one table.
 */
public class TableLoadResult {

    public int rowCount;
    public int errorCount;
    public Exception fatalException = null;

}
