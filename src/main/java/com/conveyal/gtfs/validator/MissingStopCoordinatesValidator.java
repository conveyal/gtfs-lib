package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.error.StopMissingCoordinatesError;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.validator.service.GeoUtils;
import com.conveyal.gtfs.validator.service.ProjectedCoordinate;
import com.vividsolutions.jts.geom.Coordinate;

/**
 * Created by landon on 5/11/16.
 */
public class MissingStopCoordinatesValidator extends GTFSValidator {
    @Override
    public boolean validate(GTFSFeed feed, boolean repair) {
        boolean isValid = true;
        for (Stop stop : feed.stops.values()) {
            Coordinate stopCoord = new Coordinate(stop.stop_lat, stop.stop_lon);

            ProjectedCoordinate projectedStopCoord = null;
            try {
                projectedStopCoord = GeoUtils.convertLatLonToEuclidean(stopCoord);
            } catch (IllegalArgumentException iae) {
                feed.errors.add(new StopMissingCoordinatesError(stop.stop_id, stop));
                isValid = false;
            }
        }
        return isValid;
    }
}
