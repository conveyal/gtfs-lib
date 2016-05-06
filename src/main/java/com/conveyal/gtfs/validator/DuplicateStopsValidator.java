package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.error.DuplicateStopError;
import com.conveyal.gtfs.error.StopMissingCoordinatesError;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.validator.model.DuplicateStops;
import com.conveyal.gtfs.validator.model.InvalidValue;
import com.conveyal.gtfs.validator.model.Priority;
import com.conveyal.gtfs.validator.model.ValidationResult;
import com.conveyal.gtfs.validator.service.GeoUtils;
import com.conveyal.gtfs.validator.service.ProjectedCoordinate;
import com.vividsolutions.jts.geom.Coordinate;
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
public class DuplicateStopsValidator extends GTFSValidator {
    static GeometryFactory geometryFactory = new GeometryFactory();
    private static Double buffer = 2.0;

    public boolean validate(GTFSFeed feed, boolean repair, Double bufferDistance) {
        if (bufferDistance != null) {
            buffer = bufferDistance;
        }
        return validate(feed, repair);
    }

    @Override
    public boolean validate(GTFSFeed feed, boolean repair) {
        ValidationResult result = new ValidationResult();

        Collection<Stop> stops = feed.stops.values();

        STRtree stopIndex = new STRtree();

        HashMap<String, Geometry> stopProjectedGeomMap = new HashMap<String, Geometry>();

        for(Stop stop : stops) {

            Coordinate stopCoord = new Coordinate(stop.stop_lat, stop.stop_lon);

            ProjectedCoordinate projectedStopCoord = null;

            try {
                projectedStopCoord = GeoUtils.convertLatLonToEuclidean(stopCoord);
            } catch (IllegalArgumentException iae) {
                feed.errors.add(new StopMissingCoordinatesError(stop.stop_id, stop));
                result.add(new InvalidValue("stop", "duplicateStops", stop.toString(), "MissingCoordinates", "stop " + stop + " is missing coordinates", null, Priority.MEDIUM));
            }

            Geometry geom = geometryFactory.createPoint(projectedStopCoord);

            stopIndex.insert(geom.getEnvelopeInternal(), stop);

            stopProjectedGeomMap.put(stop.stop_id, geom);

        }

        stopIndex.build();

        List<DuplicateStops> duplicateStops = new ArrayList<DuplicateStops>();

        for(Geometry stopGeom : stopProjectedGeomMap.values()) {

            Geometry bufferedStopGeom = stopGeom.buffer(buffer);

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
                                feed.errors.add(new DuplicateStopError("stop", 0, "stop_lat,stop_lon", duplicateStop.toString(), duplicateStop));
                                result.add(new InvalidValue("stop", "stop_lat,stop_lon", duplicateStop.getStopIds(), "DuplicateStops", duplicateStop.toString(), duplicateStop, Priority.LOW));
                            }
                        }

                    }
                }
            }
        }
        return false;
    }
}
