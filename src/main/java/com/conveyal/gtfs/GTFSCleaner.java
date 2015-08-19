package com.conveyal.gtfs;

import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Transfer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class GTFSCleaner {

    private static final Logger LOG = LoggerFactory.getLogger(GTFSCleaner.class);

    public static void main (String[] args) {

        if (args.length < 1) {
            System.out.printf("specify a GTFS feed to load.");
        }
        String inputFile = args[0];
            
        GTFSFeed feed = GTFSFeed.fromFile(inputFile);

        System.out.println("Finding stops used by at least one trip...");
        Map<String, Stop> usedStops = new HashMap<>();
        Set<String> retainedTripIds = new HashSet<>();

        for (String tripId : feed.trips.keySet()) {
            Iterable<StopTime> orderedStopTimes = feed.getOrderedStopTimesForTrip(tripId);
            // In-order traversal of StopTimes within this trip. The 2-tuple keys determine ordering.
            for (StopTime stopTime : orderedStopTimes) {
                String stopId = stopTime.stop_id;
                Stop stop = feed.stops.get(stopId);
                usedStops.put(stopId, stop);
                retainedTripIds.add(tripId);
            }
        }

        feed.stops.clear();
        feed.stops.putAll(usedStops);

        System.out.println("Filtering unused stoptimes...");
        Iterator<StopTime> iterator = feed.stop_times.values().iterator();
        while (iterator.hasNext()) {
            StopTime st = iterator.next();
            if ( ! retainedTripIds.contains(st.trip_id)) {
                iterator.remove();
            }
        }

        System.out.println("Filtering transfers for removed stops...");
        Iterator<Transfer> ti = feed.transfers.values().iterator();
        while (ti.hasNext()) {
            Transfer t = ti.next();
            if (usedStops.containsKey(t.from_stop.stop_id) && usedStops.containsKey(t.to_stop.stop_id)) {
                System.out.println("Keeping " + t.toString());
                continue;
            } else {
                System.out.println("Rejecting " + t.toString());
                ti.remove();
            }
        }

        if (args.length == 2) {
            System.out.println("Writing GTFS...");
            feed.toFile(args[1]);
        }

        feed.db.close();
    }

}
