package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.*;
import com.conveyal.gtfs.util.Util;
import com.google.common.collect.Iterables;
import org.locationtech.jts.geom.Coordinate;
import org.mapdb.Fun;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TODO re-implement as a TripValidator
 */
public class ReversedTripValidator extends TripValidator {

    private static Double distanceMultiplier = 1.0;

    public ReversedTripValidator(Feed feed, SQLErrorStorage errorStorage) {
        super(feed, errorStorage);
    }

    @Override
    public void validateTrip(
        Trip trip,
        Route route,
        List<StopTime> stopTimes,
        List<Stop> stops,
        List<Location> locations,
        List<LocationGroup> locationGroups
    ) {
        // TODO implement
    }

    public boolean validate(Feed feed, boolean repair) {
        boolean isValid = true;
        int errorLimit = 5000;
        int missingShapeErrorCount = 0;
        int missingCoordinatesErrorCount = 0;
        int reversedTripShapeErrorCount = 0;
        Map<ShapePoint, List<String>> missingShapesMap = new HashMap<>();

        for(Trip trip : feed.trips) {

            String tripId = trip.trip_id;
            if (trip.shape_id == null) {
                isValid = false;
                if (missingShapeErrorCount < errorLimit) {
                    // FIXME store MissingShape errors
                    // feed.errors.add(new MissingShapeError(trip));
                }
                missingShapeErrorCount++;
                continue;
            }
            String shapeId = trip.shape_id;
            Iterable<StopTime> stopTimes = feed.stopTimes.getOrdered(tripId);
            StopTime firstStop = Iterables.get(stopTimes, 0);

            StopTime lastStop = Iterables.getLast(stopTimes);

            ShapePoint firstShape = null; // feed.shape_points.ceilingEntry(Fun.t2(shapeId, null)).getValue();
            Map.Entry<Fun.Tuple2<String, Integer>, ShapePoint> entry = null; //feed.shape_points.floorEntry(new Fun.Tuple2(shapeId, Fun.HI));
            ShapePoint lastShape = entry.getValue();

            Coordinate firstStopCoord;
            Coordinate lastStopCoord;
            Coordinate firstShapeCoord;
            Coordinate lastShapeCoord;

            // if coordinate creation fails here, add trip_id to missing shapes list
            try {
                firstStopCoord = new Coordinate(feed.stops.get(firstStop.stop_id).stop_lat, feed.stops.get(firstStop.stop_id).stop_lon);
                lastStopCoord = new Coordinate(feed.stops.get(lastStop.stop_id).stop_lat, feed.stops.get(lastStop.stop_id).stop_lon);

                firstShapeCoord = new Coordinate(firstShape.shape_pt_lat, firstShape.shape_pt_lon);
                lastShapeCoord = new Coordinate(lastShape.shape_pt_lat, lastShape.shape_pt_lon);

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

            Double distanceFirstStopToStart = Util.fastDistance(
                firstStopCoord.y,
                firstStopCoord.x,
                firstShapeCoord.y,
                firstShapeCoord.x
            );
            Double distanceFirstStopToEnd = Util.fastDistance(
                firstStopCoord.y,
                firstStopCoord.x,
                lastShapeCoord.y,
                lastShapeCoord.x
            );

            Double distanceLastStopToEnd = Util.fastDistance(
                lastStopCoord.y,
                lastStopCoord.x,
                lastShapeCoord.y,
                lastShapeCoord.x
            );
            Double distanceLastStopToStart = Util.fastDistance(
                lastStopCoord.y,
                lastStopCoord.x,
                firstShapeCoord.y,
                firstShapeCoord.x
            );

            // check if first stop is x times closer to end of shape than the beginning or last stop is x times closer to start than the end
            if (distanceFirstStopToStart > (distanceFirstStopToEnd * distanceMultiplier) && distanceLastStopToEnd > (distanceLastStopToStart * distanceMultiplier)) {
                if (reversedTripShapeErrorCount < errorLimit) {
                    // FIXME store ReversedTripShape errors
                    // feed.errors.add(new ReversedTripShapeError(trip));
                }
                reversedTripShapeErrorCount++;
                isValid = false;
            }
        }
        if (missingCoordinatesErrorCount > 0) {
            for (Map.Entry<ShapePoint, List<String>> shapeError : missingShapesMap.entrySet()) {
                String[] tripIdList = shapeError.getValue().toArray(new String[shapeError.getValue().size()]);
                // FIXME store ShapeMissingCoordinates errors
                // feed.errors.add(new ShapeMissingCoordinatesError(shapeError.getKey(), tripIdList));
            }
        }
        return isValid;
    }
}
