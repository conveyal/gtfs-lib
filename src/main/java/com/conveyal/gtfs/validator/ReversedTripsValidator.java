package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.error.MissingShapeError;
import com.conveyal.gtfs.error.ReversedTripShapeError;
import com.conveyal.gtfs.error.ShapeMissingCoordinatesError;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import com.conveyal.gtfs.validator.model.InvalidValue;
import com.conveyal.gtfs.validator.model.Priority;
import com.conveyal.gtfs.validator.model.ValidationResult;
import com.conveyal.gtfs.validator.service.GeoUtils;
import com.google.common.collect.Iterables;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.mapdb.Fun;

import java.util.Collection;
import java.util.HashMap;

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
        ValidationResult result = new ValidationResult();

        Collection<Trip> trips = feed.trips.values();


        for(Trip trip : trips) {

            String tripId = trip.trip_id;
            if (trip.shape_id == null) {
                InvalidValue iv = new InvalidValue("trip", "shape_id", tripId, "MissingShape", "Trip " + tripId + " is missing a shape", null, Priority.MEDIUM);
                iv.route = trip.route;
                result.add(iv);
                isValid = false;
                feed.errors.add(new MissingShapeError(tripId));
                continue;
            }
            String shapeId = trip.shape_id;
            Iterable<StopTime> stopTimes = feed.getOrderedStopTimesForTrip(tripId);
            StopTime firstStop = Iterables.get(stopTimes, 0);

            StopTime lastStop = Iterables.getLast(stopTimes);

            Coordinate firstStopCoord = null;
            Coordinate lastStopCoord = null;
            Geometry firstShapeGeom = null;
            Geometry lastShapeGeom = null;
            Geometry firstStopGeom = null;
            Geometry lastStopGeom = null;
            Coordinate firstShapeCoord = null;
            Coordinate lastShapeCoord = null;
            try {
                firstStopCoord = new Coordinate(feed.stops.get(firstStop.stop_id).stop_lat, feed.stops.get(firstStop.stop_id).stop_lon);
                lastStopCoord = new Coordinate(feed.stops.get(lastStop.stop_id).stop_lat, feed.stops.get(lastStop.stop_id).stop_lon);

                firstStopGeom = geometryFactory.createPoint(GeoUtils.convertLatLonToEuclidean(firstStopCoord));
                lastStopGeom = geometryFactory.createPoint(GeoUtils.convertLatLonToEuclidean(lastStopCoord));

                firstShapeCoord = new Coordinate(feed.shapePoints.get(Fun.t2(shapeId, 0)).shape_pt_lat, feed.shapePoints.get(Fun.t2(shapeId, 0)).shape_pt_lon);
                lastShapeCoord = new Coordinate(feed.shapePoints.get(Fun.t2(shapeId, Fun.HI)).shape_pt_lat, feed.shapePoints.get(Fun.t2(shapeId, Fun.HI)).shape_pt_lon);

                firstShapeGeom = geometryFactory.createPoint(GeoUtils.convertLatLonToEuclidean(firstShapeCoord));
                lastShapeGeom = geometryFactory.createPoint(GeoUtils.convertLatLonToEuclidean(lastShapeCoord));
            } catch (Exception any) {
                InvalidValue iv = new InvalidValue("trip", "shape_id", tripId, "MissingCoordinates", "Trip " + tripId + " is missing coordinates", null, Priority.MEDIUM);
                iv.route = trip.route;
                result.add(iv);
                isValid = false;
                feed.errors.add(new ShapeMissingCoordinatesError(tripId, shapeId));
                continue;
            }


            firstShapeCoord = new Coordinate(feed.shapePoints.get(Fun.t2(shapeId, 0)).shape_pt_lat, feed.shapePoints.get(Fun.t2(shapeId, 0)).shape_pt_lon);
            lastShapeCoord = new Coordinate(feed.shapePoints.get(Fun.t2(shapeId, Fun.HI)).shape_pt_lat, feed.shapePoints.get(Fun.t2(shapeId, Fun.HI)).shape_pt_lon);

            firstShapeGeom = geometryFactory.createPoint(GeoUtils.convertLatLonToEuclidean(firstShapeCoord));
            lastShapeGeom = geometryFactory.createPoint(GeoUtils.convertLatLonToEuclidean(lastShapeCoord));

            Double distanceFirstStopToStart = firstStopGeom.distance(firstShapeGeom);
            Double distanceFirstStopToEnd = firstStopGeom.distance(lastShapeGeom);

            Double distanceLastStopToEnd = lastStopGeom.distance(lastShapeGeom);
            Double distanceLastStopToStart = lastStopGeom.distance(firstShapeGeom);

            // check if first stop is x times closer to end of shape than the beginning or last stop is x times closer to start than the end
            if(distanceFirstStopToStart > (distanceFirstStopToEnd * distanceMultiplier) && distanceLastStopToEnd > (distanceLastStopToStart * distanceMultiplier)) {
                InvalidValue iv =
                        new InvalidValue("trip", "shape_id", tripId, "ReversedTripShape", "Trip " + tripId + " references reversed shape " + shapeId, null, Priority.MEDIUM);
                iv.route = trip.route;
                result.add(iv);
                feed.errors.add(new ReversedTripShapeError(tripId, shapeId));
                isValid = false;
            }
        }

        return isValid;
    }
}
