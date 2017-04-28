package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.conveyal.gtfs.error.NewGTFSErrorType.*;
import static com.conveyal.gtfs.util.Util.fastDistance;

/**
 * Check that the travel times between adjacent stops in trips are reasonable.
 * This is very messy in SQL because it involves computing a function across adjacent rows in an ordered table.
 * So we do it by iterating over the whole table in Java.
 *
 * This is going to replace HopSpeedsReasonableValidator, OverlappingTripValidator, TripTimesValidator,
 * ReversedTripValidator and UnusedStopsValidator. ReversedTrips should be considered a shape validation.
 */
public class NewTripTimesValidator extends FeedValidator {

    private static final Logger LOG = LoggerFactory.getLogger(NewTripTimesValidator.class);

    public static final double MIN_SPEED = 0.1;

    int tripCount = 0;

    Feed feed;

    Set<String> referencedStops = new HashSet<>();
    Set<String> referencedTrips = new HashSet<>();
    Set<String> referencedRoutes = new HashSet<>();

    // Caching stops and trips gives a massive speed improvement by avoiding database calls. TODO add caching to the table reader class.
    Map<String, Stop> stopById = new HashMap<>();
    Map<String, Trip> tripById = new HashMap<>();

    private final TripValidator[] tripValidators = new TripValidator[] {
        new SpeedTripValidator(),
        new ReferencesTripValidator(),
        new OverlappingTripValidator(),
        new ReversedTripValidator()
    };

    @Override
    public boolean validate(Feed feed, boolean repair) {
        this.feed = feed;
        LOG.info("Cacheing stops and trips...");
        for (Stop stop : feed.stops) stopById.put(stop.stop_id, stop);
        for (Trip trip: feed.trips) tripById.put(trip.trip_id, trip);
        LOG.info("Done.");
        // Accumulate StopTimes with the same trip_id into a list, then process each trip separately.
        List<StopTime> stopTimesForTrip = new ArrayList<>();
        String previousTripId = null;
        for (StopTime stopTime : feed.stopTimes) {
            // FIXME all bad references should already be caught elsewhere, this should just be a continue
            if (stopTime.trip_id == null) continue;
            if (!stopTime.trip_id.equals(previousTripId) && !stopTimesForTrip.isEmpty()) {
                processTrip(stopTimesForTrip);
                stopTimesForTrip.clear();
            }
            stopTimesForTrip.add(stopTime);
            previousTripId = stopTime.trip_id;
        }
        processTrip(stopTimesForTrip);
        checkUnreferenced();
        for (TripValidator tripValidator : tripValidators) this.errors.addAll(tripValidator.complete());
        return errors.size() > 0;
    }

    /**
     * Check if any stops, trips, or routes were not referenced.
     */
    private void checkUnreferenced () {
        LOG.info("Finding unreferenced entities...");
        for (Stop stop : feed.stops) {
            if (!referencedStops.contains(stop.stop_id)) {
                registerError(STOP_UNUSED, stop.stop_id, stop);
            }
        }
        for (Trip trip : feed.trips) {
            if (!referencedTrips.contains(trip.trip_id)) {
                registerError(TRIP_EMPTY, trip.trip_id, trip);
            }
        }
        for (Route route : feed.routes) {
            if (!referencedRoutes.contains(route.route_id)) {
                registerError(ROUTE_UNUSED, route.route_id, route);
            }
        }
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
     * @return true if all values are OK
     */
    private boolean checkDistanceAndTime (double distanceMeters, double travelTimeSeconds, StopTime stopTime) {
        boolean good = true;
        if (distanceMeters == 0) {
            registerError(TRAVEL_DISTANCE_ZERO, null, stopTime);
            good = false;
        }
        if (travelTimeSeconds < 0) {
            registerError(TRAVEL_TIME_NEGATIVE, Double.toString(travelTimeSeconds), stopTime);
            good = false;
        } else if (travelTimeSeconds == 0) {
            registerError(TRAVEL_TIME_ZERO, null, stopTime);
            good = false;
        }
        return good;
    }

    /**
     * The first and last StopTime in a trip should have both arrival and departure times.
     * If has only one or the other, we infer them. If it's missing both we have a problem.
     * @return whether the error is not recoverable because both stoptimes are missing.
     */
    private boolean fixInitialFinal (StopTime stopTime) {
        if (missingEitherTime(stopTime)) {
            registerError(MISSING_ARRIVAL_OR_DEPARTURE, null, stopTime);
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

        if (++tripCount % 20_000 == 0) LOG.info("Validating trip {}", tripCount);

        // stopTimes should never be null, it's called only by our own code.
        if (stopTimes.size() < 2) {
            registerError(TRIP_TOO_FEW_STOP_TIMES, null, null); // TODO get the trip object in here
            return;
        }

        // Make a list of stops based on the StopTimes for this trip.
        // We could ask the SQL server to do the join between stop_times and stops, but we want to check references.
        List<Stop> stops = new ArrayList<>();
        for (Iterator<StopTime> it = stopTimes.iterator(); it.hasNext(); ) {
            StopTime stopTime = it.next();
            Stop stop = stopById.get(stopTime.stop_id); //feed.stops.get(stopTime.stop_id); // FIXME this is probably ultraslow
            if (stop == null) {
                // All bad references should have been recorded at import, we can just remove them from the trips.
                it.remove();
            } else {
                referencedStops.add(stopTime.stop_id);
                stops.add(stop);
            }
        }
        // StopTimes list may have shrunk due to missing stop references.
        if (stopTimes.size() < 2) return;

///        for (TripValidator tripValidator : tripValidators) tripValidator.validateTrip(feed, null, stopTimes);

        String tripId = stopTimes.get(0).trip_id;
        referencedTrips.add(tripId);
        Trip trip = tripById.get(tripId);


        // All bad references should have been recorded at import, we can just ignore nulls.
        Route route = null;
        if (trip != null) {
            referencedRoutes.add(trip.route_id);
            route = feed.routes.get(trip.route_id);
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
                // Both arrival and departure time were missing. Other than accumulating distance, skip this StopTime.
                continue;
            }
            if (currStopTime.departure_time < currStopTime.arrival_time) {
                registerError(DEPARTURE_BEFORE_ARRIVAL, Integer.toString(currStopTime.departure_time), currStopTime);
            }
            double travelTimeSeconds = currStopTime.arrival_time - prevStopTime.departure_time;
            if (checkDistanceAndTime(distanceMeters, travelTimeSeconds, currStopTime)) {
                // If distance and time are OK, we've got valid numbers to calculate a travel speed.
                double metersPerSecond = distanceMeters / travelTimeSeconds;
                if (metersPerSecond < MIN_SPEED || metersPerSecond > maxSpeed) {
                    NewGTFSErrorType type = (metersPerSecond < MIN_SPEED) ? TRAVEL_TOO_SLOW : TRAVEL_TOO_FAST;
                    double threshold = (metersPerSecond < MIN_SPEED) ? MIN_SPEED : maxSpeed;
                    String badValue = String.format("distance=%.0f meters; time=%.0f seconds; speed=%.1f m/sec; threshold=%.0f m/sec",
                            distanceMeters, travelTimeSeconds, metersPerSecond, threshold);
                    registerError(type, badValue, prevStopTime, currStopTime);
                }
            }
            // Reset accumulated distance, we've processed a stop time with arrival or departure time specified.
            distanceMeters = 0;
            // Record current stop and stopTime for the next iteration.
            prevStopTime = currStopTime;
            prevStop = currStop;
        }
    }

    // FIXME what is this patternId? This seems like a subset of block overlap errors (within a service day).
//            String patternId = feed.tripPatternMap.get(tripId);
//            String patternName = feed.patterns.get(patternId).name;
//            int firstDeparture = Iterables.get(stopTimes, 0).departure_time;
//            int lastArrival = Iterables.getLast(stopTimes).arrival_time;
//
//            String tripKey = trip.service_id + "_"+ blockId + "_" + firstDeparture +"_" + lastArrival + "_" + patternId;
//
//            if (duplicateTripHash.containsKey(tripKey)) {
//                String firstDepartureString = LocalTime.ofSecondOfDay(Iterables.get(stopTimes, 0).departure_time % 86399).toString();
//                String lastArrivalString = LocalTime.ofSecondOfDay(Iterables.getLast(stopTimes).arrival_time % 86399).toString();
//                String duplicateTripId = duplicateTripHash.get(tripKey);
//                Trip duplicateTrip = feed.trips.get(duplicateTripId);
//                long line = trip.sourceFileLine > duplicateTrip.sourceFileLine ? trip.sourceFileLine : duplicateTrip.sourceFileLine;
//                feed.errors.add(new DuplicateTripError(trip, line, duplicateTripId, patternName, firstDepartureString, lastArrivalString));
//                isValid = false;
//            } else {
//                duplicateTripHash.put(tripKey, tripId);
//            }


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
