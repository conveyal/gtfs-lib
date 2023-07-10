package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.Location;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopArea;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import com.google.common.collect.Lists;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.conveyal.gtfs.error.NewGTFSErrorType.*;

/**
 * This validator checks for unused entities in the dataset.
 *
 * It iterates over each trip and collects all of the route, trip, and stop IDs referenced by all of the trips found
 * within the feed. In the completion stage of the validator it verifies that there are no stops, trips, or routes in
 * the feed that do not actually get used by at least one trip.
 *
 * Created by abyrd on 2017-04-18
 */
public class ReferencesTripValidator extends TripValidator {

    Set<String> referencedStops = new HashSet<>();
    Set<String> referencedTrips = new HashSet<>();
    Set<String> referencedRoutes = new HashSet<>();
    Set<String> referencedLocations = new HashSet<>();
    Set<String> referencedStopAreas = new HashSet<>();

    public ReferencesTripValidator(Feed feed, SQLErrorStorage errorStorage) {
        super(feed, errorStorage);
    }

    @Override
    public void validateTrip(
        Trip trip,
        Route route,
        List<StopTime> stopTimes,
        List<Stop> stops,
        List<Location> locations,
        List<StopArea> stopAreas
    ) {
        if (trip != null) referencedTrips.add(trip.trip_id);
        if (route != null) referencedRoutes.add(route.route_id);
        for (Stop stop : stops) {
            if (stop == null) {
                continue;
            }
            referencedStops.add(stop.stop_id);
            // If a stop used by the trip has a parent station, count this among the referenced stops, too. While the
            // parent station may not be referenced directly, the relationship is functioning correctly and there is
            // not an issue with this stop being unreferenced.
            if (stop.parent_station != null) referencedStops.add(stop.parent_station);
        }
        locations.forEach(location -> {
            if (location != null) {
                referencedLocations.add(location.location_id);
            }
        });
        stopAreas.forEach(stopArea -> {
            if (stopArea != null) {
                referencedStopAreas.add(stopArea.area_id);
            }
        });
    }

    @Override
    public void complete (ValidationResult validationResult) {
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
        // A location is used as a stop id within stop times. If the stop id is a location id check for a match against
        // the referenced locations.
        List<Location> locations = Lists.newArrayList(feed.locations);
        feed.stopTimes.forEach(stopTime -> {
            if (FlexValidator.stopIdIsLocation(stopTime.stop_id, locations) &&
                !referencedLocations.contains(stopTime.stop_id)
            ) {
                registerError(getLocationById(locations, stopTime.stop_id), LOCATION_UNUSED, stopTime.stop_id);
            }
        });

        // A stop area is used as a stop id within stop times. If the stop id is a stop area check for a
        // match against the referenced stop areas.
        List<StopArea> stopAreas = Lists.newArrayList(feed.stopAreas);
        feed.stopTimes.forEach(stopTime -> {
            if (FlexValidator.stopIdIsStopArea(stopTime.stop_id, stopAreas) &&
                !referencedStopAreas.contains(stopTime.stop_id)
            ) {
                registerError(
                    getStopAreaById(stopAreas, stopTime.stop_id),
                    STOP_AREA_UNUSED,
                    stopTime.stop_id
                );
            }
        });
    }

    /**
     * Get location by location id or return null if there is no match.
     */
    private Location getLocationById(List<Location> locations, String locationId) {
        return locations.stream()
            .filter(location -> locationId.equals(location.location_id))
            .findAny()
            .orElse(null);
    }

    /**
     * Get stop area by area id or return null if there is no match.
     */
    private StopArea getStopAreaById(List<StopArea> stopAreas, String areaId) {
        return stopAreas.stream()
            .filter(stopArea -> areaId.equals(stopArea.area_id))
            .findAny()
            .orElse(null);
    }

}
