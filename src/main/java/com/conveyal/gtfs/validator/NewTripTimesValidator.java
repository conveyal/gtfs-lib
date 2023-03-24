package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.Entity;
import com.conveyal.gtfs.model.Location;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopArea;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.conveyal.gtfs.error.NewGTFSErrorType.CONDITIONALLY_REQUIRED;
import static com.conveyal.gtfs.error.NewGTFSErrorType.MISSING_ARRIVAL_OR_DEPARTURE;
import static com.conveyal.gtfs.error.NewGTFSErrorType.TRIP_TOO_FEW_STOP_TIMES;

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

    int tripCount = 0;
    boolean skipStandardTripValidation = false;

    // Caching stops and trips gives a massive speed improvement by avoiding database calls.
    // TODO build this same kind of caching into the table reader class.
    Map<String, Stop> stopById = new HashMap<>();
    Map<String, Location> locationById = new HashMap<>();
    Map<String, StopArea> stopAreaById = new HashMap<>();
    Map<String, Trip> tripById = new HashMap<>();
    Map<String, Route> routeById = new HashMap<>();

    // As an optimization, these validators are fed the stoptimes for each trip to avoid repeated iteration and grouping.
    private final TripValidator[] standardTripValidators;
    private final TripValidator[] additionalTripValidators;

    public NewTripTimesValidator(Feed feed, SQLErrorStorage errorStorage) {
        super(feed, errorStorage);
        standardTripValidators = new TripValidator[] {
            new SpeedTripValidator(feed, errorStorage),
            new ReferencesTripValidator(feed, errorStorage),
            new ReversedTripValidator(feed, errorStorage),
        };
        additionalTripValidators = new TripValidator[] {
            new ServiceValidator(feed, errorStorage),
            new PatternFinderValidator(feed, errorStorage)
        };
    }

    @Override
    public void validate () {
        // TODO cache automatically in feed or TableReader object
        LOG.info("Cacheing stops, trips, and routes...");
        for (Stop stop : feed.stops) {
            stopById.put(stop.stop_id, stop);
        }
        for (Location location : feed.locations) {
            locationById.put(location.location_id, location);
        }
        for (StopArea stopArea : feed.stopAreas) {
            stopAreaById.put(stopArea.area_id, stopArea);
        }
        // FIXME: determine a good way to validate shapes without caching them all in memory...
        for (Trip trip: feed.trips) {
            tripById.put(trip.trip_id, trip);
        }
        for (Route route: feed.routes) {
            routeById.put(route.route_id, route);
        }
        LOG.info("Done.");
        // Accumulate StopTimes with the same trip_id into a list, then process each trip separately.
        List<StopTime> stopTimesForTrip = new ArrayList<>();
        String previousTripId = null;
        // Order stop times by trip ID and sequence number (i.e. scan through the stops in each trip in order)
        for (StopTime stopTime : feed.stopTimes.getAllOrdered()) {
            // All bad references should already be caught elsewhere, this should just be a continue
            if (stopTime.trip_id == null) continue;
            if (!stopTime.trip_id.equals(previousTripId) && !stopTimesForTrip.isEmpty()) {
                processTrip(stopTimesForTrip);
                stopTimesForTrip.clear();
            }
            stopTimesForTrip.add(stopTime);
            previousTripId = stopTime.trip_id;
        }
        if (!stopTimesForTrip.isEmpty()) processTrip(stopTimesForTrip);
    }

    protected static boolean missingEitherTime(StopTime stopTime) {
        return (stopTime.arrival_time == Entity.INT_MISSING || stopTime.departure_time == Entity.INT_MISSING);
    }

    protected static boolean missingBothTimes(StopTime stopTime) {
        return (stopTime.arrival_time == Entity.INT_MISSING && stopTime.departure_time == Entity.INT_MISSING);
    }

    /**
     * If the StopTime is missing one of arrival or departure time, copy from the other field.
     */
    protected static void fixMissingTimes(StopTime stopTime) {
        if (stopTime.arrival_time == Entity.INT_MISSING) {
            stopTime.arrival_time = stopTime.departure_time;
        }
        if (stopTime.departure_time == Entity.INT_MISSING) {
            stopTime.departure_time = stopTime.arrival_time;
        }
    }

    /**
     * The first and last StopTime in a trip should have both arrival and departure times.
     * If it has only one or the other, we infer them. If it's missing both we have a problem.
     */
    private void fixInitialFinal(StopTime stopTime) {
        if (missingEitherTime(stopTime)) {
            registerError(stopTime, MISSING_ARRIVAL_OR_DEPARTURE);
            fixMissingTimes(stopTime);
            if (missingEitherTime(stopTime)) {
                //TODO: Is this even needed? Already covered by MISSING_ARRIVAL_OR_DEPARTURE.
                registerError(stopTime, CONDITIONALLY_REQUIRED, "First and last stop times are required to have both an arrival and departure time.");
            }
        }
    }

    /**
     * This validates an ordered list of stopTimes for a single trip. This should only be called with non-null stopTimes.
     * @param stopTimes must all have the same trip_id and be in order of increasing stop_sequence
     */
    private void processTrip (List<StopTime> stopTimes) {
        if (++tripCount % 20_000 == 0) LOG.info("Validating trip {}", tripCount);
        // All stop times have the same trip_id, so we look it up right away.
        // FIXME: gtfs_load error if there are no stop times? / feed=Birnie_Bus_20141105T102949-05_24e99790-211d-4f92-b1d2-147e6f3d5040.zip
        String tripId = stopTimes.get(0).trip_id;
        Trip trip = tripById.get(tripId);
        if (trip == null) {
            // This feed does not contain a trip with the ID specified in these stop_times.
            // This error should already have been caught TODO verify.
            return;
        }

        boolean hasContinuousBehavior = false;
        // Make a parallel list of stops based on the stop_times for this trip.
        // We will remove any stop_times for stops that don't exist in the feed.
        // We could ask the SQL server to do the join between stop_times and stops, but we want to check references.
        List<Stop> stops = new ArrayList<>();
        List<Location> locations = new ArrayList<>();
        List<StopArea> stopAreas = new ArrayList<>();
        for (Iterator<StopTime> it = stopTimes.iterator(); it.hasNext(); ) {
            StopTime stopTime = it.next();
            if (hasContinuousBehavior(stopTime.continuous_drop_off, stopTime.continuous_pickup)) {
                hasContinuousBehavior = true;
            }
            Stop stop = stopById.get(stopTime.stop_id);
            Location location = locationById.get(stopTime.stop_id);
            StopArea stopArea = stopAreaById.get(stopTime.stop_id);
            if (stop == null && location == null && stopArea == null) {
                // All bad references should have been recorded at import, we can just remove them from the trips.
                it.remove();
            } else {
                if (stop == null && location == null) {
                    stopAreas.add(stopArea);
                } else if (stop == null && stopArea == null) {
                    locations.add(location);
                } else {
                    stops.add(stop);
                }
            }
        }

        // If either of these conditions are true none of the trip validators' validateTrip methods are executed.
        if (hasSingleFlexStop(stopTimes, locations, stopAreas)) {
            LOG.warn("Trip has a single flex stop.");
            skipStandardTripValidation = true;
            return;
        } else if (hasSingleStop(stopTimes, locations, stopAreas)) {
            LOG.warn("Too few stop times that have references to stops to validate trip.");
            registerError(trip, TRIP_TOO_FEW_STOP_TIMES);
            return;
        }

        // Check that first and last stop times are not missing values and repair them if they are not locations or
        // location groups. Note that this repair will be seen by the validators but not saved in the database.
        StopTime firstStop = stopTimes.get(0);
        StopTime lastStop = stopTimes.get(stopTimes.size() - 1);
        if (!FlexValidator.stopIdIsStopAreaOrLocation(firstStop.stop_id, stopAreas, locations)) {
            fixInitialFinal(firstStop);
        }
        if (!FlexValidator.stopIdIsStopAreaOrLocation(lastStop.stop_id, stopAreas, locations)) {
            fixInitialFinal(lastStop);
        }

        for (StopTime stopTime : stopTimes) {
            if (!FlexValidator.stopIdIsStopAreaOrLocation(stopTime.stop_id, stopAreas, locations)) {
                // Repair the case where an arrival or departure time is provided, but not both.
                fixMissingTimes(stopTime);
            }
        }
        // TODO check characteristics of timepoints
        // All bad references should have been recorded at import and null trip check is handled above, we can just
        // ignore nulls.
        Route route = routeById.get(trip.route_id);
        if (route != null &&
            hasContinuousBehavior(route.continuous_drop_off, route.continuous_pickup)) {
            hasContinuousBehavior = true;
        }

        if (trip.shape_id == null && hasContinuousBehavior) {
            registerError(
                trip,
                CONDITIONALLY_REQUIRED,
                "shape_id is required when a trip has continuous behavior defined."
            );
        }

        // Pass these same cleaned lists of stop_times and stops into each trip validator in turn.
        for (TripValidator tripValidator : standardTripValidators) {
            tripValidator.validateTrip(trip, route, stopTimes, stops, locations, stopAreas);
        }
        for (TripValidator tripValidator : additionalTripValidators) {
            tripValidator.validateTrip(trip, route, stopTimes, stops, locations, stopAreas);
        }
    }

    /**
     * Completing this feed validator means completing each of its constituent trip validators. This is the case even
     * if none of the trip validators' validateTrip methods are called. This is required as some complete methods have
     * additional processing beyond validation.
     */
    public void complete (ValidationResult validationResult) {
        for (TripValidator tripValidator : additionalTripValidators) {
            LOG.info("Running complete stage for {}", tripValidator.getClass().getSimpleName());
            tripValidator.complete(validationResult);
            LOG.info("{} finished", tripValidator.getClass().getSimpleName());
        }
        if (!skipStandardTripValidation) {
            for (TripValidator tripValidator : standardTripValidators) {
                LOG.info("Running complete stage for {}", tripValidator.getClass().getSimpleName());
                tripValidator.complete(validationResult);
                LOG.info("{} finished", tripValidator.getClass().getSimpleName());
            }
        } else {
            LOG.warn("Skipping trip validators due to restrictions being imposed.");
        }
    }

    /**
     * Determine if a trip has continuous behaviour by checking the values that have been defined for continuous drop
     * off and pickup.
     */
    private boolean hasContinuousBehavior(int continuousDropOff, int continuousPickup) {
        return
            continuousDropOff == 0 ||
            continuousDropOff == 2 ||
            continuousDropOff == 3 ||
            continuousPickup == 0 ||
            continuousPickup == 2 ||
            continuousPickup == 3;
    }

    /**
     * A single stop is permitted if it is a location or location group.
     */
    private boolean hasSingleFlexStop(
        List<StopTime> stopTimes,
        List<Location> locations,
        List<StopArea> stopAreas
    ) {
        return stopTimes.size() < 2 && (!locations.isEmpty() || !stopAreas.isEmpty());
    }

    /**
     * A single stop is not permitted. At least two stop times must be provided.
     */
    private boolean hasSingleStop(
         List<StopTime> stopTimes,
         List<Location> locations,
         List<StopArea> stopAreas
    ) {
        return stopTimes.size() < 2 && locations.isEmpty() && stopAreas.isEmpty();
    }

}
