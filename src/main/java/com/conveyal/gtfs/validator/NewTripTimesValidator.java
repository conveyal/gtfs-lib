package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.GTFSError;
import com.conveyal.gtfs.error.GeneralError;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import org.apache.commons.math3.util.FastMath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Check that the travel times between adjacent stops in trips are reasonable.
 * This is very messy in SQL because it involves computing a function across adjacent rows in an ordered table.
 * So we do it by iterating over the whole table in Java.
 *
 * This is going to replace HopSpeedsReasonableValidator, OverlappingTripsValidator, TripTimesValidator,
 * ReversedTripsValidator and UnusedStopsValidator. ReversedTrips should be considered a shape validation.
 */
public class NewTripTimesValidator extends Validator {

    private static final Logger LOG = LoggerFactory.getLogger(NewTripTimesValidator.class);

    public static final double MIN_SPEED = 0.1;

    int tripCount = 0;

    Feed feed;

    Set<String> referencedStops = new HashSet<>();
    Set<String> referencedTrips = new HashSet<>();
    Set<String> referencedRoutes = new HashSet<>();

    // Caching stops and routes gives a massive speed improvement by avoiding database calls. TODO add caching to the table reader class.
    Map<String, Stop> stopById = new HashMap<>();
    Map<String, Route> routeById = new HashMap<>();

    @Override
    public boolean validate(Feed feed, boolean repair) {
        this.feed = feed;

        LOG.info("Cacheing stops and routes...");
        for (Stop stop : feed.stops) stopById.put(stop.stop_id, stop);
        for (Route route: feed.routes) routeById.put(route.route_id, route);
        LOG.info("Done.");

        // Accumulate StopTimes with the same trip_id into a list, then process each trip separately.
        List<StopTime> stopTimesForTrip = new ArrayList<>();
        String previousTripId = null;
        for (StopTime stopTime : feed.stopTimes) {
            if (stopTime.trip_id == null) {
                registerError("Stop time missing trip ID");
                continue;
            }
            if (!stopTime.trip_id.equals(previousTripId) && !stopTimesForTrip.isEmpty()) {
                processTrip(stopTimesForTrip);
                stopTimesForTrip.clear();
            }
            stopTimesForTrip.add(stopTime);
            previousTripId = stopTime.trip_id;
        }
        processTrip(stopTimesForTrip);
        checkUnreferenced();
        return errors.size() > 0;
    }

    /**
     * Check if any stops or trips were not referenced
     */
    private void checkUnreferenced () {
        LOG.info("Finding unreferenced entities...");
        Set<String> unreferencedStopIds = new HashSet<>(stopById.keySet());
        unreferencedStopIds.removeAll(referencedStops);
        // FIXME remove stations, or check that all stations were referenced.
        for (String stopId : unreferencedStopIds) registerError("Stop was not referenced by any trip times: " + stopId);
        Set<String> unreferencedTripIds = new HashSet<>();
        for (Trip trip : feed.trips) unreferencedTripIds.add(trip.trip_id); // FIXME Is this slow?
        unreferencedTripIds.removeAll(referencedTrips);
        for (String tripId : unreferencedTripIds) registerError("Trip was not referenced by any trip times: " + tripId);
        Set<String> unreferencedRouteIds = new HashSet<>(routeById.keySet());
        unreferencedRouteIds.removeAll(referencedRoutes);
        for (String routeId : unreferencedRouteIds) registerError("Route was not referenced by any trip times: " + routeId);
    }

    private static boolean missingEitherTime (StopTime stopTime) {
        return (stopTime.arrival_time == -1 || stopTime.departure_time == -1);
    }

    private static boolean missingBothTimes (StopTime stopTime) {
        return (stopTime.arrival_time == -1 && stopTime.departure_time == -1);
    }

    /**
     * If the StopTime is missing one of arrival or departure time, copy from the other field.
     * @return whether one of the times was missing.
     */
    private boolean fixMissingTimes (StopTime stopTime) {
        boolean missing = false;
        if (stopTime.arrival_time == -1) {
            stopTime.arrival_time = stopTime.departure_time;
            missing = true;
        }
        if (stopTime.departure_time == -1) {
            stopTime.departure_time = stopTime.arrival_time;
            missing = true;
        }
        return missing;
    }

    /**
     * This just pulls some of the range checking logic out of the main trip checking loop so it's more readable.
     */
    private boolean checkDistanceAndTime (double distanceMeters, double travelTimeSeconds) {
        boolean bad = false;
        if (distanceMeters == 0) {
            registerError("Distance between two successive stops is zero.");
            bad = true;
        }
        if (travelTimeSeconds < 0) {
            registerError("Travel time between two successive stops is negative.");
            bad = true;
        } else if (travelTimeSeconds == 0) {
            registerError("Travel time between two successive stops is zero.");
            bad = true;
        }
        return bad;
    }

    /**
     * The first and last StopTime in a trip should have both arrival and departure times.
     * If has only one or the other, we infer them. If it's missing both we have a problem.
     * @return whether the error is not recoverable because both stoptimes are missing.
     */
    private boolean fixInitialFinal (StopTime stopTime) {
        if (missingEitherTime(stopTime)) {
            registerError("First and last stops in trip must have both arrival and departure time.");
            fixMissingTimes(stopTime);
            if (missingEitherTime(stopTime)) {
                return true;
            }
        }
        return false;
    }

    /**
     * This validates an ordered list of stopTimes for a single trip.
     * @param stopTimes must all have the same trip_id and be in order of increasing stop_sequence
     */
    private void processTrip (List<StopTime> stopTimes) {

        if (++tripCount % 10_000 == 0) LOG.info("Validating trip {}", tripCount);

        // stopTimes should never be null, it's called only by our own code.
        if (stopTimes.size() < 2) {
            registerError("Trip needs at least two stops.");
            return;
        }

        // Make a list of stops based on the StopTimes for this trip.
        // We could ask the SQL server to do the join between stop_times and stops, but we want to check references.
        List<Stop> stops = new ArrayList<>();
        for (Iterator<StopTime> it = stopTimes.iterator(); it.hasNext(); ) {
            StopTime stopTime = it.next();
            Stop stop = stopById.get(stopTime.stop_id); //feed.stops.get(stopTime.stop_id); // FIXME this is probably ultraslow
            if (stop == null) {
                registerError("Stop time references a stop that doesn't exist.");
                it.remove();
            } else {
                referencedStops.add(stopTime.stop_id);
                stops.add(stop);
            }
        }
        // StopTimes list may have shrunk due to missing stop references.
        if (stopTimes.size() < 2) return;

        Route route = null;
        String tripId = stopTimes.get(0).trip_id;
        Trip trip = feed.trips.get(tripId);
        if (trip == null) {
            registerError("stop_times references an unknown trip: " + tripId);
        } else {
            referencedTrips.add(trip.trip_id);
            route = routeById.get(trip.route_id); //feed.routes.get(trip.route_id);
            if (route != null) referencedRoutes.add(route.route_id);
        }
        // The specific maximum speed for this trip's route's mode of travel.
        double maxSpeed = getMaxSpeed(route);

        // Check that first and last stop times are not missing values.
        fixInitialFinal(stopTimes.get(0));
        fixInitialFinal(stopTimes.get(stopTimes.size() - 1));
        // Chop off any missing times from the beginning of the trip so we can properly validate the rest of it.
        while (missingBothTimes(stopTimes.get(0))) stopTimes = stopTimes.subList(1, stopTimes.size());
        if (stopTimes.size() < 2) return;

        // TODO check characteristics of timepoints

        // Unfortunately we can't work on each stop pair in isolation,
        // because we want to accumulate distance when stop times are missing.
        StopTime prevStopTime = stopTimes.get(0);
        Stop prevStop = stops.get(0);
        double distanceMeters = 0;
        for (int i = 1; i < stopTimes.size(); i++) {
            StopTime currStopTime = stopTimes.get(i);
            Stop currStop = stops.get(i);
            // Distance is accumulated in case times are not provided for some StopTimes.
            distanceMeters += fastDistance(currStop.stop_lat, currStop.stop_lon, prevStop.stop_lat, prevStop.stop_lon);
            fixMissingTimes(currStopTime);
            if (missingEitherTime(currStopTime)) {
                // Both arrival or departure time were missing. Other than accumulating distance, skip this StopTime.
                continue;
            }
            double travelTimeSeconds = currStopTime.arrival_time - prevStopTime.departure_time;
            if (checkDistanceAndTime(distanceMeters, travelTimeSeconds)) {
                // We've got valid numbers to calculate a travel speed.
                double metersPerSecond = distanceMeters / travelTimeSeconds;
                if (metersPerSecond < MIN_SPEED) registerError("Vehicle going too slow.");
                if (metersPerSecond > maxSpeed) registerError("Vehicle going too fast.");
            }
            // Reset accumulated distance, we've processed a stop time with arrival or departure time specified.
            distanceMeters = 0;
            // Record current stop and stopTime for the next iteration.
            prevStopTime = currStopTime;
            prevStop = currStop;
        }
    }

    private static final double M_PER_DEGREE_LAT = 111111.111; // Using 18th century meters, 10e6 meters / 90 degrees.

    /**
     * @return Equirectangular approximation to distance.
     */
    private static double fastDistance (double lat0, double lon0, double lat1, double lon1) {
        double midLat = (lat0 + lat1) / 2;
        double xscale = FastMath.cos(FastMath.toRadians(midLat));
        double dx = xscale * (lon1 - lon0);
        double dy = (lat1 - lat0);
        return FastMath.sqrt(dx * dx + dy * dy) * M_PER_DEGREE_LAT;
    }

    private double getMaxSpeed (Route route) {
        int type = -1;
        if (route != null) type = route.route_type;
        switch (type) {
            case Route.SUBWAY:
                return 40; // 40 m/sec is about 140 kph, speed of HK airport line
            case Route.RAIL:
                return 84; // HSR max speed is around 300kph, about 84 m/sec
            case Route.FERRY:
                return 30; // World's fastest ferry is 107kph or 30 m/sec
            default:
                return 36; // 130 kph is max highway speed, about 36 m/sec
        }
    }

}
