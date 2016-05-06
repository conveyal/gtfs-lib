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

        HashMap<String, String> duplicateTripHash = new HashMap<String, String>();

        for(Trip trip : feed.trips.values()) {

            String tripId = trip.trip_id;

            Iterable<StopTime> stopTimes = feed.getOrderedStopTimesForTrip(tripId);

            if(stopTimes == null || Iterables.size(stopTimes) == 0) {
                InvalidValue iv = new InvalidValue("trip", "trip_id", tripId, "NoStopTimesForTrip", "Trip Id " + tripId + " has no stop times." , null, Priority.HIGH);
                iv.route = trip.route;
                result.add(iv);
                feed.errors.add(new NoStopTimesForTripError(tripId));
                isValid = false;
                continue;
            }

            StopTime previousStopTime = null;
            for(StopTime stopTime : stopTimes) {

                if(stopTime.departure_time < stopTime.arrival_time) {
                    InvalidValue iv =
                            new InvalidValue("stop_time", "trip_id", tripId, "StopTimeDepartureBeforeArrival", "Trip Id " + tripId + " stop sequence " + stopTime.stop_sequence + " departs before arriving.", null, Priority.HIGH);
                    iv.route = trip.route;
                    result.add(iv);
                    feed.errors.add(new StopTimeDepartureBeforeArrivalError(tripId, stopTime.stop_sequence));
                    isValid = false;
                }

                if(previousStopTime != null) {

                    if(stopTime.arrival_time < previousStopTime.departure_time) {
                        InvalidValue iv =
                                new InvalidValue("stop_time", "trip_id", tripId, "StopTimesOutOfSequence", "Trip Id " + tripId + " stop sequence " + stopTime.stop_sequence + " arrives before departing " + previousStopTime.stop_sequence, null, Priority.HIGH);
                        iv.route = trip.route;
                        feed.errors.add(new StopTimesOutOfSequenceError(tripId, stopTime.stop_sequence, previousStopTime.stop_sequence));
                        result.add(iv);

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

            String tripKey = trip.service.service_id + "_"+ blockId + "_" + Iterables.get(stopTimes, 0).departure_time +"_" + Iterables.getLast(stopTimes).arrival_time + "_" + stopIds;

            if(duplicateTripHash.containsKey(tripKey)) {
                String duplicateTripId = duplicateTripHash.get(tripKey);
                InvalidValue iv =
                        new InvalidValue("trip", "trip_id", tripId, "DuplicateTrip", "Trip Ids " + duplicateTripId + " & " + tripId + " are duplicates (" + tripKey + ")" , null, Priority.LOW);
                iv.route = trip.route;
                feed.errors.add(new DuplicateTripError(tripId, duplicateTripId, tripKey));
                result.add(iv);
                isValid = false;

            }
            else
                duplicateTripHash.put(tripKey, tripId);


        }

        return isValid;
    }
}
