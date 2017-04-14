package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.error.DuplicateStopError;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.storage.StorageException;
import com.conveyal.gtfs.validator.model.DuplicateStops;
import com.conveyal.gtfs.validator.service.GeoUtils;
import com.conveyal.gtfs.validator.service.ProjectedCoordinate;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.index.strtree.STRtree;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

/**
 * Created by landon on 5/2/16.
 */
public class DuplicateStopsValidator extends Validator {

    static GeometryFactory geometryFactory = new GeometryFactory();
    private static Double buffer = 2.0; // in meters

    /**
     * REVIEW
     * This does the following:
     * Makes a spatial index of all the _unprojected_ stop coordinates.
     * Projects each stop one by one and puts them in a map.
     * Iterates over all the projected stop coordinates.
     * Buffers each one by 2 meters.
     * Queries the lat/lon spatial index using this projected buffered coordinate (wrong)
     * If more than one stop is found in this radius, it then does a cross product of all found stops.
     *
     * How could the feed IDs be different? This is only supposed to check within one feed.
     * Checking if something has already been found by iteration over a list instead of using a hashmap.
     * There are almost no comments.
     * Need to just iterate over upper-triangular stop pairs found.
     */
    @Override
    public boolean validate (Feed feed, boolean repair) {
        boolean isValid = true;
        STRtree stopIndex = feed.getStopSpatialIndex();
        HashMap<String, Geometry> stopProjectedGeomMap = new HashMap<String, Geometry>();
        for (Stop stop : feed.stops) {
            Coordinate stopCoord = new Coordinate(stop.stop_lat, stop.stop_lon);
            ProjectedCoordinate projectedStopCoord = null;
            try {
                projectedStopCoord = GeoUtils.convertLatLonToEuclidean(stopCoord);
            } catch (IllegalArgumentException iae) {
                // FIXME silent consumption of exceptions? Why?
            }
            Geometry geom = geometryFactory.createPoint(projectedStopCoord);
            stopProjectedGeomMap.put(stop.stop_id, geom);
        }

        List<DuplicateStops> duplicateStops = new ArrayList<DuplicateStops>();

        // FIXME silently checking if the stopIndex is missing - why would it be missing and shouldn't we handle that?
        if (stopIndex != null) {
            for(Geometry stopGeom : stopProjectedGeomMap.values()) {

                Geometry bufferedStopGeom = stopGeom.buffer(buffer); // FIXME units on variable name

                List<Stop> stopCandidates = (List<Stop>)stopIndex.query(bufferedStopGeom.getEnvelopeInternal());

                if(stopCandidates.size() > 1) {

                    for(Stop stop1 : stopCandidates) {
                        for(Stop stop2 : stopCandidates) {

                            if(stop1.stop_id != stop2.stop_id) {

                                Boolean stopPairAlreadyFound = false;
                                for(DuplicateStops duplicate : duplicateStops) {

                                    if((duplicate.stop1.feed_id.equals(stop1.feed_id) && duplicate.stop2.feed_id.equals(stop2.feed_id)) ||
                                            (duplicate.stop2.feed_id.equals(stop1.feed_id) && duplicate.stop1.feed_id.equals(stop2.feed_id)))
                                        stopPairAlreadyFound = true;
                                }

                                if(stopPairAlreadyFound)
                                    continue;

                                Geometry stop1Geom = stopProjectedGeomMap.get(stop1.stop_id);
                                Geometry stop2Geom = stopProjectedGeomMap.get(stop2.stop_id);

                                double distance = stop1Geom.distance(stop2Geom);

                                // if stopDistance is within bufferDistance consider duplicate
                                if(distance <= buffer){

                                    // TODO: a good place to check if stops are part of a station grouping

                                    DuplicateStops duplicateStop = new DuplicateStops(stop1, stop2, distance);
                                    duplicateStops.add(duplicateStop);
                                    isValid = false;
                                    feed.errors.add(new DuplicateStopError(duplicateStop));
                                }
                            }

                        }
                    }
                }
            }
        }
        return isValid;
    }

}
