package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.error.MisplacedStopError;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.validator.service.GeoUtils;
import com.conveyal.gtfs.validator.service.ProjectedCoordinate;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.index.strtree.STRtree;

import java.util.List;

/**
 * Created by landon on 5/11/16.
 */
public class MisplacedStopValidator extends GTFSValidator {

    @Override
    public boolean validate(GTFSFeed feed, boolean repair) {
        boolean isValid = true;
        Envelope nullIsland = new Envelope(-1, 1, -1, 1);
        STRtree spatialIndex = feed.getSpatialIndex();
        GeometryFactory geometryFactory = new GeometryFactory();
        long index = 1;

        for (Stop stop : feed.stops.values()) {
            try {
                Coordinate stopCoord = new Coordinate(stop.stop_lon, stop.stop_lat);

                // Check if stop is in null island
                if (nullIsland.contains(stopCoord)) {
                    feed.errors.add(new MisplacedStopError(stop.stop_id, index, stop));
                    isValid = false;
                    continue;
                }
                ProjectedCoordinate projectedStopCoord = null;

                try {
                    projectedStopCoord = GeoUtils.convertLatLonToEuclidean(stopCoord);
                } catch (IllegalArgumentException iae) {
                    continue;
                }
                Double bufferDistance = 1.0;
                Geometry geom = geometryFactory.createPoint(projectedStopCoord);
                Geometry bufferedStopGeom = geom.buffer(bufferDistance);

                // TODO: Check for nearest neighbor
                /*
                For each stop, compute the nearest neighbor distance, excluding any stops that are within (say) 500m
                so that way we don’t find distances of 0m when there are a bunch of stops piled up at null island.
                if that distance is more than 100km, the stop is an outlier
                i.e. if there are no stops between 500m and 100km away, it’s far from the transit service
                */
//                spatialIndex.nearestNeighbour();
//                List<Stop> stopCandidates = (List<Stop>)spatialIndex.query(bufferedStopGeom.getEnvelopeInternal());

            } catch (Exception e) {
                continue;
            }
            index++;
        }
        return isValid;
    }
}
