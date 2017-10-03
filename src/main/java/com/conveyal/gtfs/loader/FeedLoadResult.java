package com.conveyal.gtfs.loader;

/**
 * An instance of this class is returned by the GTFS feed loading method.
 * It provides a summary of what happened during the loading process.
 * It also provides the unique name that was assigned to this feed by the loader.
 * That unique name is the name of a database schema including all the tables loaded from this feed.
 */
public class FeedLoadResult {

    public String uniqueIdentifier;
    public int errorCount;
    public Exception fatalException;

    public TableLoadResult agency;
    public TableLoadResult calendar;
    public TableLoadResult calendarDates;
    public TableLoadResult fareAttributes;
    public TableLoadResult fareRules;
    public TableLoadResult feedInfo;
    public TableLoadResult frequencies;
    public TableLoadResult routes;
    public TableLoadResult shapes;
    public TableLoadResult stops;
    public TableLoadResult stopTimes;
    public TableLoadResult transfers;
    public TableLoadResult trips;

}
