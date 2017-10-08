package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;
import com.google.common.base.Joiner;
import com.vividsolutions.jts.geom.LineString;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class Pattern implements Serializable {
    public static final long serialVersionUID = 1L;

    // A unique ID for this jouney pattern / stop pattern
    public String pattern_id;

    // The segment of the pattern's geometry (which is always a LineString) on which each stop in the sequence falls.
    public int[] segmentIndex;

    // The percentage in [0..1] along the line segment at which each stop in the sequence falls.
    public double[] segmentFraction;

    public List<String> orderedStops;
    // TODO: change list of trips to set
    public List<String> associatedTrips;
    // TODO: add set of shapes
//    public Set<String> associatedShapes;
    public LineString geometry;
    public String name;
    public String route_id;
    public static Joiner joiner = Joiner.on("-").skipNulls();
    public String feed_id;

    // TODO: Should a Pattern be generated for a single trip or a set of trips that share the same ordered stop list?

    /**
     *
     * @param orderedStops
     * @param trips the first trip will serve as an exemplar for all the others.
     * @param patternGeometry
     */
    public Pattern (List<String> orderedStops, Collection<Trip> trips, LineString patternGeometry){

        // TODO do we need to store a feed ID?
        //this.feed_id = feed.feedId;

        // Temporarily make a random ID for the pattern, which might be overwritten in a later step ?
        this.pattern_id = UUID.randomUUID().toString();

        // Assign ordered list of stop IDs to be the key of this pattern.
        // FIXME what about pickup / dropoff type?
        this.orderedStops = orderedStops;

        // Save the string IDs of the trips on this pattern.
        this.associatedTrips = trips.stream().map(t -> t.trip_id).collect(Collectors.toList());

        // In theory all trips could take different paths and be on different routes.
        // Here we're using only the first one as an exemplar.
        String trip_id = associatedTrips.get(0);

        Trip exemplarTrip = trips.get(0);
        this.geometry = patternGeometry;

        // feed.getTripGeometry(exemplarTrip.trip_id);

        // Patterns have one and only one route.
        // FIXME are we certain we're only passing in trips on one route? or are we losing information here?
        this.route_id = exemplarTrip.route_id;

        // A name is assigned to this pattern based on the headsign, short name, direction ID or stop IDs.
        // This is not at all guaranteed to be unique, it's just to help identify the pattern.
        if (exemplarTrip.trip_headsign != null){
            name = exemplarTrip.trip_headsign;
        }
        else if (exemplarTrip.trip_short_name != null) {
            name = exemplarTrip.trip_short_name;
        }
        else if (exemplarTrip.direction_id >= 0){
            name = String.valueOf(exemplarTrip.direction_id);
        }
        else{
            name = joiner.join(orderedStops);
        }

        // TODO: Implement segmentIndex using JTS to segment out LineString by stops.

        // TODO: Implement segmentFraction using JTS to segment out LineString by stops.

    }
}
