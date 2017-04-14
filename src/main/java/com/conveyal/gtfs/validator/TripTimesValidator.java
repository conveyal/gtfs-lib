package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.DuplicateTripError;
import com.conveyal.gtfs.error.NoStopTimesForTripError;
import com.conveyal.gtfs.error.StopTimeDepartureBeforeArrivalError;
import com.conveyal.gtfs.error.StopTimesOutOfSequenceError;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import com.google.common.collect.Iterables;

import java.time.LocalTime;
import java.util.HashMap;

/**
 * When ordering trips by stop_sequence, stoptimes should be increasing, speeds within range.
 * Trips should have at least one hop (at least two stops)
 * Speed should not be infinite (check distance, time separately)
 */
public class TripTimesValidator extends Validator {

    public static final int ERROR_LIMIT = 2000;

    @Override
    public boolean validate(Feed feed, boolean repair) {

        boolean isValid = true;
        int noStopTimesErrorCount = 0;
        int stopTimeDepartureBeforeArrivalErrorCount = 0;
        int stopTimesOutOfSequenceErrorCount = 0;
        int duplicateTripErrorCount = 0;
        HashMap<String, String> duplicateTripHash = new HashMap<>();

        for (Trip trip : feed.trips) {
            String tripId = trip.trip_id;
            Iterable<StopTime> stopTimes = feed.stopTimes.getOrdered(tripId);
            if (stopTimes == null || Iterables.size(stopTimes) == 0) {
                if (noStopTimesErrorCount < ERROR_LIMIT) {
                    feed.errors.add(new NoStopTimesForTripError(trip));
                }
                isValid = false;
                noStopTimesErrorCount++;
                continue;
            }

            StopTime previousStopTime = null;
            for (StopTime stopTime : stopTimes) {

                // check for out of order departures and arrivals
                if(stopTime.departure_time < stopTime.arrival_time) {
                    if (stopTimeDepartureBeforeArrivalErrorCount < ERROR_LIMIT) {
                        feed.errors.add(new StopTimeDepartureBeforeArrivalError(stopTime));
                    }
                    stopTimeDepartureBeforeArrivalErrorCount++;
                    isValid = false;
                }

                // check if arrival time is missing (e.g., non-timepoint)
                if (stopTime.arrival_time == Integer.MIN_VALUE) {
                    continue;
                }

                // check if previous time is null
                if (previousStopTime != null) {
                    // check for out of sequence stop times
                    if(stopTime.arrival_time < previousStopTime.departure_time) {
                        if (stopTimesOutOfSequenceErrorCount < ERROR_LIMIT) {
                            feed.errors.add(new StopTimesOutOfSequenceError(stopTime, previousStopTime));
                        }
                        stopTimesOutOfSequenceErrorCount++;
                        isValid = false;

                        // only capturing first out of sequence stop for now -- could consider collapsing duplicates based on tripId
                        break;
                    }

                }
                previousStopTime = stopTime;

                // break out of validator if error count equals limit and we're not repairing feed
                if (!repair && stopTimesOutOfSequenceErrorCount >= ERROR_LIMIT) {
                    break;
                }
            }

            // check for duplicate trips starting at the same time with the same service id

            String blockId = "";

            if (trip.block_id != null) blockId = trip.block_id;

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

            // break out of validator if error count equals limit and we're not repairing feed
            if (!repair && duplicateTripErrorCount >= ERROR_LIMIT) {
                break;
            }
        }
        return isValid;
    }
}
