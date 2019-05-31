package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.Entity;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.ShapePoint;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.conveyal.gtfs.error.NewGTFSErrorType.*;

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

    // Caching stops and trips gives a massive speed improvement by avoiding database calls.
    // TODO build this same kind of caching into the table reader class.
//    ListMultimap<String, ShapePoint> shapeById = MultimapBuilder.treeKeys().arrayListValues().build();
    Map<String, Stop> stopById = new HashMap<>();
    Map<String, Trip> tripById = new HashMap<>();
    Map<String, Route> routeById = new HashMap<>();

    // As an optimization, these validators are fed the stoptimes for each trip to avoid repeated iteration and grouping.
    private final TripValidator[] tripValidators;

    public NewTripTimesValidator(Feed feed, SQLErrorStorage errorStorage) {
        super(feed, errorStorage);
        tripValidators = new TripValidator[] {
            new SpeedTripValidator(feed, errorStorage),
            new ReferencesTripValidator(feed, errorStorage),
            new ReversedTripValidator(feed, errorStorage),
            new ServiceValidator(feed, errorStorage),
            new PatternFinderValidator(feed, errorStorage)
        };
    }

    @Override
    public void validate () {
        // TODO cache automatically in feed or TableReader object
        LOG.info("Cacheing stops, trips, and routes...");
        for (Stop stop : feed.stops) stopById.put(stop.stop_id, stop);
        // FIXME: determine a good way to validate shapes without caching them all in memory...
//        for (ShapePoint shape : feed.shapePoints.getAllOrdered()) shapeById.put(shape.shape_id, shape);
        for (Trip trip: feed.trips) tripById.put(trip.trip_id, trip);
        for (Route route: feed.routes) routeById.put(route.route_id, route);
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

    protected static boolean missingEitherTime (StopTime stopTime) {
        return (stopTime.arrival_time == Entity.INT_MISSING || stopTime.departure_time == Entity.INT_MISSING);
    }

    protected static boolean missingBothTimes(StopTime stopTime) {
        return (stopTime.arrival_time == Entity.INT_MISSING && stopTime.departure_time == Entity.INT_MISSING);
    }

    /**
     * If the StopTime is missing one of arrival or departure time, copy from the other field.
     * @return whether one of the times was missing.
     */
    protected static boolean fixMissingTimes (StopTime stopTime) {
        boolean missing = false;
        if (stopTime.arrival_time == Entity.INT_MISSING) {
            stopTime.arrival_time = stopTime.departure_time;
            missing = true;
        }
        if (stopTime.departure_time == Entity.INT_MISSING) {
            stopTime.departure_time = stopTime.arrival_time;
            missing = true;
        }
        return missing;
    }

    /**
     * The first and last StopTime in a trip should have both arrival and departure times.
     * If has only one or the other, we infer them. If it's missing both we have a problem.
     * @return whether the error is not recoverable because both stoptimes are missing.
     */
    private boolean fixInitialFinal (StopTime stopTime) {
        if (missingEitherTime(stopTime)) {
            registerError(stopTime, MISSING_ARRIVAL_OR_DEPARTURE);
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
        // All stop times have the same trip_id, so we look it up right away.
        // FIXME: gtfs_load error if there are no stop times? / feed=Birnie_Bus_20141105T102949-05_24e99790-211d-4f92-b1d2-147e6f3d5040.zip
        String tripId = stopTimes.get(0).trip_id;
        Trip trip = tripById.get(tripId);
        if (trip == null) {
            // This feed does not contain a trip with the ID specified in these stop_times.
            // This error should already have been caught TODO verify.
            return;
        }
        // Our code should only call this method with non-null stopTimes.
        if (stopTimes.size() < 2) {
            registerError(trip, TRIP_TOO_FEW_STOP_TIMES);
            return;
        }
        // Make a parallel list of stops based on the stop_times for this trip.
        // We will remove any stop_times for stops that don't exist in the feed.
        // We could ask the SQL server to do the join between stop_times and stops, but we want to check references.
        List<Stop> stops = new ArrayList<>();
        for (Iterator<StopTime> it = stopTimes.iterator(); it.hasNext(); ) {
            StopTime stopTime = it.next();
            Stop stop = stopById.get(stopTime.stop_id);
            if (stop == null) {
                // All bad references should have been recorded at import, we can just remove them from the trips.
                it.remove();
            } else {
                stops.add(stop);
            }
        }
        // StopTimes list may have shrunk due to missing stop references.
        if (stopTimes.size() < 2) return;
        // Check that first and last stop times are not missing values and repair them.
        // Note that this repair will be seen by the validators but not saved in the database.
        fixInitialFinal(stopTimes.get(0));
        fixInitialFinal(stopTimes.get(stopTimes.size() - 1));
        // Repair the case where an arrival or departure time is provided, but not both.
        for (StopTime stopTime : stopTimes) fixMissingTimes(stopTime);
        // TODO check characteristics of timepoints
        // All bad references should have been recorded at import and null trip check is handled above, we can just
        // ignore nulls.
        Route route = routeById.get(trip.route_id);
        // Pass these same cleaned lists of stop_times and stops into each trip validator in turn.
        for (TripValidator tripValidator : tripValidators) tripValidator.validateTrip(trip, route, stopTimes, stops);
    }

    /**
     * Completing this feed validator means completing each of its constituent trip validators.
     */
    public void complete (ValidationResult validationResult) {
        for (TripValidator tripValidator : tripValidators) {
            LOG.info("Running complete stage for {}", tripValidator.getClass().getSimpleName());
            tripValidator.complete(validationResult);
            LOG.info("{} finished", tripValidator.getClass().getSimpleName());
        }
    }

}
