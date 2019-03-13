package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.conveyal.gtfs.error.NewGTFSErrorType.TRIP_OVERLAP_IN_BLOCK;

/**
 * Created by landon on 5/2/16.
 */
public class OverlappingTripValidator extends TripValidator {
    private static final Logger LOG = LoggerFactory.getLogger(OverlappingTripValidator.class);
    // check for overlapping trips within block
    private HashMap<String, List<BlockInterval>> blockIntervals = new HashMap<>();

    public OverlappingTripValidator(Feed feed, SQLErrorStorage errorStorage) {
        super(feed, errorStorage);
    }

    @Override
    public void validateTrip(Trip trip, Route route, List<StopTime> stopTimes, List<Stop> stops) {
        if (trip.block_id != null) {
            // If the trip has a block_id, add a new block interval to the
            BlockInterval blockInterval = new BlockInterval();
            blockInterval.trip = trip;
            StopTime firstStopTime = stopTimes.get(0);
            blockInterval.startTime = firstStopTime.departure_time;
            blockInterval.firstStop = firstStopTime;
            blockInterval.lastStop = stopTimes.get(stopTimes.size() - 1);
            // Construct new list of intervals if none exists for encountered block_id.
            blockIntervals
                .computeIfAbsent(trip.block_id, k -> new ArrayList<>())
                .add(blockInterval);
        }
    }

    @Override
    public void complete (ValidationResult validationResult) {
        // Iterate over each block and determine if there are any trips that overlap one another.
        for (String blockId : blockIntervals.keySet()) {
            List<BlockInterval> intervals = blockIntervals.get(blockId);
            intervals.sort(Comparator.comparingInt(i -> i.startTime));
            // Iterate over each interval (except for the last) comparing it to every other interval (so the last interval
            // is handled through the course of iteration).
            // FIXME this has complexity of n^2, there has to be a better way.
            for (int n = 0; n < intervals.size() - 1; n++) {
                BlockInterval interval1 = intervals.get(n);
                // Compare the interval at position N with all other intervals at position N+1 to the end of the list.
                for (BlockInterval interval2 : intervals.subList(n + 1, intervals.size())) {
                    if (interval1.lastStop.departure_time <= interval2.firstStop.arrival_time || interval2.lastStop.departure_time <= interval1.firstStop.arrival_time) {
                        continue;
                    }
                    // If either trip's last departure occurs after the other's first arrival, they overlap. We still
                    // need to determine if they operate on the same day though.
                    if (interval1.trip.service_id.equals(interval2.trip.service_id)) {
                        // If the overlapping trips share a service_id, record an error.
                        registerError(interval1.trip, TRIP_OVERLAP_IN_BLOCK, interval2.trip.trip_id);
                    } else {
                        // Trips overlap but don't have the same service_id.
                        // Check to see if service days fall on the same days of the week.
                        ServiceValidator.ServiceInfo info1 = validationResult.serviceInfoForServiceId.get(interval1.trip.service_id);
                        ServiceValidator.ServiceInfo info2 = validationResult.serviceInfoForServiceId.get(interval2.trip.service_id);
                        Set<LocalDate> overlappingDates = new HashSet<>(info1.datesActive); // use the copy constructor
                        overlappingDates.retainAll(info2.datesActive);
                        if (overlappingDates.size() > 0) {
                            registerError(interval1.trip, TRIP_OVERLAP_IN_BLOCK, interval2.trip.trip_id);
                        }
                    }
                }
            }
        }
    }

    private class BlockInterval {
        Trip trip;
        Integer startTime;
        StopTime firstStop;
        StopTime lastStop;
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
//                long line = trip.id > duplicateTrip.id ? trip.id : duplicateTrip.id;
//                feed.errors.add(new DuplicateTripError(trip, line, duplicateTripId, patternName, firstDepartureString, lastArrivalString));
//                isValid = false;
//            } else {
//                duplicateTripHash.put(tripKey, tripId);
//            }


}

