package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.conveyal.gtfs.error.NewGTFSErrorType.*;

/**
 * Created by abyrd on 2017-04-18
 */
public class ReferencesTripValidator extends TripValidator {

    Set<String> referencedStops = new HashSet<>();
    Set<String> referencedTrips = new HashSet<>();
    Set<String> referencedRoutes = new HashSet<>();

    public ReferencesTripValidator(Feed feed, SQLErrorStorage errorStorage) {
        super(feed, errorStorage);
    }

    @Override
    public void validateTrip(Trip trip, Route route, List<StopTime> stopTimes, List<Stop> stops) {
        if (trip != null) referencedTrips.add(trip.trip_id);
        if (route != null) referencedRoutes.add(route.route_id);
        for (Stop stop : stops) referencedStops.add(stop.stop_id);
    }

    @Override
    public void complete() {
        for (Stop stop : feed.stops) {
            if (!referencedStops.contains(stop.stop_id)) {
                registerError(stop, STOP_UNUSED, stop.stop_id);
            }
        }
        for (Trip trip : feed.trips) {
            if (!referencedTrips.contains(trip.trip_id)) {
                registerError(trip, TRIP_EMPTY);
            }
        }
        for (Route route : feed.routes) {
            if (!referencedRoutes.contains(route.route_id)) {
                registerError(route, ROUTE_UNUSED);
            }
        }
    }

}
