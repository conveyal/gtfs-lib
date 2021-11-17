package com.conveyal.gtfs.loader;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * An instance of this class is returned by the GTFS feed loading method.
 * It provides a summary of what happened during the loading process.
 * It also provides the unique name that was assigned to this feed by the loader.
 * That unique name is the name of a database schema including all the tables loaded from this feed.
 *
 * Ignore unknown properties on deserialization to avoid conflicts with past versions. FIXME
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FeedLoadResult implements Serializable {

    private static final long serialVersionUID = 1L;
    public String filename;
    public String uniqueIdentifier;
    public int errorCount;
    public String fatalException;

    public TableLoadResult agency;
    public TableLoadResult bookingRules;
    public TableLoadResult calendar;
    public TableLoadResult calendarDates;
    public TableLoadResult fareAttributes;
    public TableLoadResult fareRules;
    public TableLoadResult feedInfo;
    public TableLoadResult frequencies;
    public TableLoadResult locationGroups;
    public TableLoadResult routes;
    public TableLoadResult shapes;
    public TableLoadResult stops;
    public TableLoadResult stopTimes;
    public TableLoadResult transfers;
    public TableLoadResult trips;
    public TableLoadResult translations;
    public TableLoadResult attributions;

    public long loadTimeMillis;
    public long completionTime;

    public FeedLoadResult () {
        this(false);
    }

    /**
     * Optional constructor to generate blank table load results on instantiation.
     */
    public FeedLoadResult (boolean constructTableResults) {
        agency = new TableLoadResult();
        bookingRules = new TableLoadResult();
        calendar = new TableLoadResult();
        calendarDates = new TableLoadResult();
        fareAttributes = new TableLoadResult();
        fareRules = new TableLoadResult();
        feedInfo = new TableLoadResult();
        frequencies = new TableLoadResult();
        locationGroups = new TableLoadResult();
        routes = new TableLoadResult();
        shapes = new TableLoadResult();
        stops = new TableLoadResult();
        stopTimes = new TableLoadResult();
        transfers = new TableLoadResult();
        trips = new TableLoadResult();
        translations = new TableLoadResult();
        attributions = new TableLoadResult();
    }

    /**
     * Determine if the feed loaded has GTFS Flex enhancements.
     */
    // TODO: Confirm this is adequate to determine a flex feed and add location meta data and location shapes
    //  once available.
    public boolean isGTFSFlex() {
        return
            bookingRules != null &&
            locationGroups != null &&
            bookingRules.rowCount > 0 &&
            locationGroups.rowCount > 0;
    }
}
