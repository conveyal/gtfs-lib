package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.ShapePoint;
import com.conveyal.gtfs.model.Trip;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.conveyal.gtfs.error.NewGTFSErrorType.SHAPE_SHAPE_DIST_TRAVELED_NOT_INCREASING;
import static com.conveyal.gtfs.error.NewGTFSErrorType.SHAPE_UNUSED;

/**
 * A validator that checks the integrity of the shapes records
 */
public class ShapeValidator extends FeedValidator  {
    public ShapeValidator(Feed feed, SQLErrorStorage errorStorage) {
        super(feed, errorStorage);
    }

    @Override
    public void validate() {
        ShapePoint lastShapePoint = null;
        Map<String, ShapePoint> firstShapePointByShapeId = new HashMap<>();
        // this stores all shape ids found in the shapes initially, but will eventually be modified to only have the
        // extra shape ids if there are any
        Set<String> extraShapeIds = new HashSet<>();

        for (ShapePoint shapePoint : feed.shapePoints) {
            // store the first found shapePoint when a new shape_id is found
            if (shapePoint.shape_id != null && !firstShapePointByShapeId.containsKey(shapePoint.shape_id)) {
                firstShapePointByShapeId.put(shapePoint.shape_id, shapePoint);
                extraShapeIds.add(shapePoint.shape_id);
            }

            // continue loop if first shape, or beginning analysis of new shape
            if (lastShapePoint == null || !lastShapePoint.shape_id.equals(shapePoint.shape_id)) {
                lastShapePoint = shapePoint;
                continue;
            }

            // make sure the shape distance traveled is increasing
            if (lastShapePoint.shape_dist_traveled > shapePoint.shape_dist_traveled) {
                registerError(shapePoint, SHAPE_SHAPE_DIST_TRAVELED_NOT_INCREASING, shapePoint.shape_dist_traveled);
            }

            lastShapePoint = shapePoint;
        }

        // verify that all found shapeIds exist in trips

        // compile a list of shape_ids found in the trips table
        // Optimization idea: speed up by making custom SQL call to fetch distinct shape_ids from trip table
        Set<String> tripShapeIds = new HashSet<>();
        for (Trip trip : feed.trips) {
            tripShapeIds.add(trip.shape_id);
        }

        // remove all trip shape ids from the found shape ids in the shapes table
        extraShapeIds.removeAll(tripShapeIds);

        // iterate over the extra shape Ids and create errors for each
        for (String extraShapeId : extraShapeIds) {
            registerError(firstShapePointByShapeId.get(extraShapeId), SHAPE_UNUSED, extraShapeId);
        }
    }
}
