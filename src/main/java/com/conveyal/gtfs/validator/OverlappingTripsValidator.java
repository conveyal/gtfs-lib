package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.error.OverlappingTripsInBlockError;
import com.conveyal.gtfs.model.CalendarDate;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import com.conveyal.gtfs.validator.model.InvalidValue;
import com.conveyal.gtfs.validator.model.Priority;
import com.conveyal.gtfs.validator.model.ValidationResult;
import com.google.common.collect.Iterables;
import org.mapdb.Fun;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by landon on 5/2/16.
 */
public class OverlappingTripsValidator extends GTFSValidator {
    private static Double distanceMultiplier = 1.0;

    public boolean validate(GTFSFeed feed, boolean repair, Double distanceMultiplier) {
        this.distanceMultiplier = distanceMultiplier;
        return validate(feed, repair);
    }

    @Override
    public boolean validate(GTFSFeed feed, boolean repair) {

        ValidationResult result = new ValidationResult();

        // check for overlapping trips within block
        HashMap<String, ArrayList<BlockInterval>> blockIntervals = new HashMap<String, ArrayList<BlockInterval>>();

        feed.trips.values().stream().iterator().forEachRemaining(trip -> {
            // store trip intervals by block id
            if (trip.block_id != null) {
                Iterable<StopTime> stopTimes = feed.getOrderedStopTimesForTrip(trip.trip_id);
                BlockInterval blockInterval = new BlockInterval();
                blockInterval.trip = trip;
                StopTime firstStopTime = Iterables.get(stopTimes, 0);
                blockInterval.startTime = firstStopTime.departure_time;
                blockInterval.firstStop = firstStopTime;
                blockInterval.lastStop = Iterables.getLast(stopTimes);

                if(!blockIntervals.containsKey(trip.block_id))
                    blockIntervals.put(trip.block_id, new ArrayList<BlockInterval>());

                blockIntervals.get(trip.block_id).add(blockInterval);

            }
        });

        for(String blockId : blockIntervals.keySet()) {

            ArrayList<BlockInterval> intervals = blockIntervals.get(blockId);

            Collections.sort(intervals, new BlockIntervalComparator());

            int iOffset = 0;
            for(BlockInterval i1 : intervals) {
                for(BlockInterval i2 : intervals.subList(iOffset, intervals.size() - 1)) {


                    String tripId1 = i1.trip.trip_id;
                    String tripId2 = i2.trip.trip_id;


                    if(!tripId1.equals(tripId2)) {
                        // if trips don't overlap, skip
                        if(i1.lastStop.departure_time <= i2.firstStop.arrival_time || i2.lastStop.departure_time <= i1.firstStop.arrival_time)
                            continue;

                        // if trips have same service id they overlap
                        if(i1.trip.service.service_id.equals(i2.trip.service.service_id)) {
                            // not strictly correct; they could be on different routes
                            String[] tripIds = {tripId1, tripId2};
                            feed.errors.add(new OverlappingTripsInBlockError("trip", 0, "block_id", blockId, i1.trip.route, tripIds, Priority.HIGH));
                        }

                        else {

                            // if trips don't share service id check to see if service dates fall on the same days/day of week
                            for(Map.Entry<LocalDate, CalendarDate> d1 : feed.services.get(i1.trip.service.service_id).calendar_dates.entrySet()) {

                                if(feed.services.get(i2.trip.service.service_id).calendar_dates.containsKey(d1.getKey())) {
                                    String[] tripIds = {tripId1, tripId2};
                                    feed.errors.add(new OverlappingTripsInBlockError("trip", 0, "block_id", blockId, i1.trip.route, tripIds, Priority.HIGH));
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

    private class BlockInterval implements Comparable<BlockInterval> {
        Trip trip;
        Integer startTime;
        StopTime firstStop;
        StopTime lastStop;

        public int compareTo(BlockInterval o) {
            return new Integer(this.firstStop.arrival_time).compareTo(new Integer(o.firstStop.arrival_time));
        }
    }

    private class BlockIntervalComparator implements Comparator<BlockInterval> {

        public int compare(BlockInterval a, BlockInterval b) {
            return new Integer(a.startTime).compareTo(new Integer(b.startTime));
        }
    }
}

