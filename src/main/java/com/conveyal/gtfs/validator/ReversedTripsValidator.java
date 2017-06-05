package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.error.MissingShapeError;
import com.conveyal.gtfs.error.ReversedTripShapeError;
import com.conveyal.gtfs.error.ShapeMissingCoordinatesError;
import com.conveyal.gtfs.model.ShapePoint;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import com.conveyal.gtfs.validator.service.GeoUtils;
import com.google.common.collect.Iterables;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.mapdb.tuple.Tuple;
import org.mapdb.tuple.Tuple2;

import java.util.*;

/**
 * Created by landon on 5/2/16.
 */
public class ReversedTripsValidator extends GTFSValidator {
    private static Double distanceMultiplier = 1.0;

    static GeometryFactory geometryFactory = new GeometryFactory();

    public boolean validate(GTFSFeed feed, boolean repair, Double distanceMultiplier) {
        this.distanceMultiplier = distanceMultiplier;
        return validate(feed, repair);
    }

    @Override
    public boolean validate(GTFSFeed feed, boolean repair) {
        boolean isValid = true;
        int errorLimit = 5000;
        int missingShapeErrorCount = 0;
        int missingCoordinatesErrorCount = 0;
        int reversedTripShapeErrorCount = 0;
        Collection<Trip> trips = feed.trips.values();
        Map<ShapePoint, List<String>> missingShapesMap = new HashMap<>();

        for(Trip trip : trips) {

            String tripId = trip.trip_id;
            if (trip.shape_id == null) {
                isValid = false;
                if (missingShapeErrorCount < errorLimit) {
                    feed.errors.add(new MissingShapeError(trip));
                }
                missingShapeErrorCount++;
                continue;
            }
            String shapeId = trip.shape_id;
            Iterable<StopTime> stopTimes = feed.getOrderedStopTimesForTrip(tripId);
            StopTime firstStop = Iterables.get(stopTimes, 0);

            StopTime lastStop = Iterables.getLast(stopTimes);

            ShapePoint firstShape = feed.shape_points.ceilingEntry(new Tuple2(shapeId, null)).getValue();
            Map.Entry<Tuple2<String, Integer>, ShapePoint> entry = feed.shape_points.floorEntry(new Tuple2(shapeId, Tuple.HI));
            ShapePoint lastShape = entry.getValue();

            Coordinate firstStopCoord;
            Coordinate lastStopCoord;
            Geometry firstShapeGeom;
            Geometry lastShapeGeom;
            Geometry firstStopGeom;
            Geometry lastStopGeom;
            Coordinate firstShapeCoord;
            Coordinate lastShapeCoord;

            // if coordinate creation fails here, add trip_id to missing shapes list
            try {
                firstStopCoord = new Coordinate(feed.stops.get(firstStop.stop_id).stop_lat, feed.stops.get(firstStop.stop_id).stop_lon);
                lastStopCoord = new Coordinate(feed.stops.get(lastStop.stop_id).stop_lat, feed.stops.get(lastStop.stop_id).stop_lon);

                firstStopGeom = geometryFactory.createPoint(GeoUtils.convertLatLonToEuclidean(firstStopCoord));
                lastStopGeom = geometryFactory.createPoint(GeoUtils.convertLatLonToEuclidean(lastStopCoord));

                firstShapeCoord = new Coordinate(firstShape.shape_pt_lat, firstShape.shape_pt_lon);
                lastShapeCoord = new Coordinate(lastShape.shape_pt_lat, lastShape.shape_pt_lon);

                firstShapeGeom = geometryFactory.createPoint(GeoUtils.convertLatLonToEuclidean(firstShapeCoord));
                lastShapeGeom = geometryFactory.createPoint(GeoUtils.convertLatLonToEuclidean(lastShapeCoord));
            } catch (Exception any) {
                isValid = false;
                List<String> listOfTrips;
                if (missingShapesMap.containsKey(firstShape)) {
                    listOfTrips = missingShapesMap.get(firstShape);
                }
                else {
                    listOfTrips = new ArrayList<String>();
                }
                listOfTrips.add(tripId);
                missingShapesMap.put(firstShape, listOfTrips);
                missingCoordinatesErrorCount++;
                continue;
            }

            Double distanceFirstStopToStart = firstStopGeom.distance(firstShapeGeom);
            Double distanceFirstStopToEnd = firstStopGeom.distance(lastShapeGeom);

            Double distanceLastStopToEnd = lastStopGeom.distance(lastShapeGeom);
            Double distanceLastStopToStart = lastStopGeom.distance(firstShapeGeom);

            // check if first stop is x times closer to end of shape than the beginning or last stop is x times closer to start than the end
            if (distanceFirstStopToStart > (distanceFirstStopToEnd * distanceMultiplier) && distanceLastStopToEnd > (distanceLastStopToStart * distanceMultiplier)) {
                if (reversedTripShapeErrorCount < errorLimit) {
                    feed.errors.add(new ReversedTripShapeError(trip));
                }
                reversedTripShapeErrorCount++;
                isValid = false;
            }
        }
        if (missingCoordinatesErrorCount > 0) {
            for (Map.Entry<ShapePoint, List<String>> shapeError : missingShapesMap.entrySet()) {
                String[] tripIdList = shapeError.getValue().toArray(new String[shapeError.getValue().size()]);
                feed.errors.add(new ShapeMissingCoordinatesError(shapeError.getKey(), tripIdList));
            }
        }
        return isValid;
    }
}
