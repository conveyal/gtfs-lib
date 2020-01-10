package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.util.Util;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineString;

import java.util.Map;

/**
 * Represents a collection of GTFS shape points. Never saved in MapDB but constructed on the fly.
 */
public class Shape {
    /** The shape itself */
    public LineString geometry;

    /** shape_dist_traveled for each point in the geometry. TODO how to handle shape dist traveled not specified, or not specified on all stops? */
    public double[] shape_dist_traveled;

    public Shape (GTFSFeed feed, String shape_id) {
        // TODO Check Mapdb3 submap syntax
        Map<Object[], ShapePoint> points =
                feed.shape_points.subMap(new Object[]{shape_id}, new Object[]{shape_id, null});

        Coordinate[] coords = points.values().stream()
                .map(point -> new Coordinate(point.shape_pt_lon, point.shape_pt_lat))
                .toArray(i -> new Coordinate[i]);
        geometry = Util.geometryFactory.createLineString(coords);
        shape_dist_traveled = points.values().stream().mapToDouble(point -> point.shape_dist_traveled).toArray();
    }
}
