package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.OverlappingTripsInBlockError;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.*;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.time.LocalDate;
import java.util.*;

import static com.conveyal.gtfs.error.NewGTFSErrorType.TRIP_OVERLAP_IN_BLOCK;

/**
 * REVIEW
 * whoa: feed.trips.values().stream().iterator().forEachRemaining(trip -> {})
 * should be for (Trip trip : feed.trips) {}
 * We're fetching all the stop times for every trip in at least three different validators.
 *
 * Created by landon on 5/2/16.
 */
public class OverlappingTripValidator extends TripValidator {

    // check for overlapping trips within block
    HashMap<String, List<BlockInterval>> blockIntervals = new HashMap<>();

    public OverlappingTripValidator(Feed feed, SQLErrorStorage errorStorage) {
        super(feed, errorStorage);
    }

    @Override
    public void validateTrip(Trip trip, Route route, List<StopTime> stopTimes, List<Stop> stops) {
        if (trip.block_id != null) {
            BlockInterval blockInterval = new BlockInterval();
            blockInterval.trip = trip;
            StopTime firstStopTime = stopTimes.get(0);
            blockInterval.startTime = firstStopTime.departure_time;
            blockInterval.firstStop = firstStopTime;
            blockInterval.lastStop = stopTimes.get(stopTimes.size() - 1);
            List<BlockInterval> intervals = blockIntervals.get(trip.block_id);
            if (intervals == null) {
                intervals = new ArrayList<>();
                blockIntervals.put(trip.block_id, intervals);
            }
            intervals.add(blockInterval);
        }
    }

    @Override
    public void complete() {
        for (String blockId : blockIntervals.keySet()) {
            List<BlockInterval> intervals = blockIntervals.get(blockId);
            Collections.sort(intervals, Comparator.comparingInt(i -> i.startTime));
            int i2offset = 0;
            // FIXME this has complexity of n^2, there has to be a better way.
            for (BlockInterval i1 : intervals) {
                // Compare the interval at position N with all other intervals at position N+1 to the end of the list.
                i2offset += 1; // Fixme replace with integer iteration and get(n)
                for(BlockInterval i2 : intervals.subList(i2offset, intervals.size() - 1)) {
                    String tripId1 = i1.trip.trip_id;
                    String tripId2 = i2.trip.trip_id;
                    if (tripId1.equals(tripId2)) {
                        // Why would this happen? Just duplicating a case covered by the original implementation.
                        // TODO can this happen at all?
                        continue;
                    }
                    // if trips don't overlap, skip FIXME can't this be simplified?
                    if (i1.lastStop.departure_time <= i2.firstStop.arrival_time || i2.lastStop.departure_time <= i1.firstStop.arrival_time) {
                        continue;
                    }
                    if (i1.trip.service_id.equals(i2.trip.service_id)) {
                        // Trips overlap. If they have the same service_id they overlap.
                        registerError(TRIP_OVERLAP_IN_BLOCK, "block_id="+i1.trip.block_id, i1.trip, i2.trip);
                    } else {
                        // Trips overlap but don't have the same service_id.
                        // Check to see if service days fall on the same days of the week.
                        // FIXME requires more complex Service implementation
                        Service s1 = null; //feed.services.get(i1.trip.service_id);
                        Service s2 = null; //feed.services.get(i2.trip.service_id);
                        boolean overlap = Service.checkOverlap(s1, s2);
                        for (Map.Entry<LocalDate, CalendarDate> d1 : s1.calendar_dates.entrySet()) {
                            LocalDate date = d1.getKey();
                            boolean activeOnDate = s1.activeOn(date) && s2.activeOn(date);
                            if (activeOnDate || overlap) { // FIXME this will always be true if overlap is true.
                                registerError(TRIP_OVERLAP_IN_BLOCK, "block_id=" + blockId, i1.trip, i2.trip);
                                break;
                            }
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
//                long line = trip.sourceFileLine > duplicateTrip.sourceFileLine ? trip.sourceFileLine : duplicateTrip.sourceFileLine;
//                feed.errors.add(new DuplicateTripError(trip, line, duplicateTripId, patternName, firstDepartureString, lastArrivalString));
//                isValid = false;
//            } else {
//                duplicateTripHash.put(tripKey, tripId);
//            }


}

