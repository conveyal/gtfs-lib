package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.error.DuplicateTripError;
import com.conveyal.gtfs.error.NoStopTimesForTripError;
import com.conveyal.gtfs.error.StopTimeDepartureBeforeArrivalError;
import com.conveyal.gtfs.error.StopTimesOutOfSequenceError;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import com.conveyal.gtfs.validator.model.InvalidValue;
import com.conveyal.gtfs.validator.model.Priority;
import com.conveyal.gtfs.validator.model.ValidationResult;
import com.google.common.collect.Iterables;

import java.util.HashMap;

public class TripTimesValidator extends GTFSValidator {

    @Override
    public boolean validate(GTFSFeed feed, boolean repair) {
        // When ordering trips by stop_sequence, stoptimes should be increasing, speeds within range
        // Trips should have at least one hop (at least two stops)
        // Speed should not be infinite (check distance, time separately)
        boolean isValid = true;
        ValidationResult result = new ValidationResult();
        int errorLimit = 2000;
        int noStopTimesErrorCount = 0;
        int stopTimeDepartureBeforeArrivalErrorCount = 0;
        int stopTimesOutOfSequenceErrorCount = 0;
        int duplicateTripErrorCount = 0;
        HashMap<String, String> duplicateTripHash = new HashMap<String, String>();

        for(Trip trip : feed.trips.values()) {

            String tripId = trip.trip_id;

            Iterable<StopTime> stopTimes = feed.getOrderedStopTimesForTrip(tripId);

            if(stopTimes == null || Iterables.size(stopTimes) == 0) {
                if (noStopTimesErrorCount < errorLimit) {
                    feed.errors.add(new NoStopTimesForTripError(tripId, trip.route_id));
                }
                isValid = false;
                noStopTimesErrorCount++;
                continue;
            }

            StopTime previousStopTime = null;
            for(StopTime stopTime : stopTimes) {

                if(stopTime.departure_time < stopTime.arrival_time) {
                    if (stopTimeDepartureBeforeArrivalErrorCount < errorLimit) {
                        feed.errors.add(new StopTimeDepartureBeforeArrivalError(tripId, stopTime.stop_sequence));
                    }
                    stopTimeDepartureBeforeArrivalErrorCount++;
                    isValid = false;
                }

                if(previousStopTime != null) {

                    if(stopTime.arrival_time < previousStopTime.departure_time) {
                        if (stopTimesOutOfSequenceErrorCount < errorLimit) {
                            feed.errors.add(new StopTimesOutOfSequenceError(tripId, stopTime.stop_sequence, previousStopTime.stop_sequence));
                        }
                        stopTimesOutOfSequenceErrorCount++;
                        isValid = false;

                        // only capturing first out of sequence stop for now -- could consider collapsing duplicates based on tripId
                        break;
                    }

                }

                previousStopTime = stopTime;
            }

            // check for duplicate trips starting at the same time with the same service id

            String stopIds = "";
            String blockId = "";

            if(trip.block_id != null)
                blockId = trip.block_id;

            for(StopTime stopTime : stopTimes) {
                if (stopTime.stop_id != null && feed.stops.get(stopTime.stop_id) != null) {
                    stopIds += stopTime.stop_id + ",";
                }
            }

            String tripKey = trip.service_id + "_"+ blockId + "_" + Iterables.get(stopTimes, 0).departure_time +"_" + Iterables.getLast(stopTimes).arrival_time + "_" + stopIds;

            if(duplicateTripHash.containsKey(tripKey)) {
                String duplicateTripId = duplicateTripHash.get(tripKey);
                feed.errors.add(new DuplicateTripError(tripId, duplicateTripId, tripKey, trip.route_id));
                isValid = false;

            }
            else
                duplicateTripHash.put(tripKey, tripId);


        }

        return isValid;
    }
}
