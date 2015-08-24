package com.conveyal.gtfs;

import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Transfer;
import com.conveyal.gtfs.model.Trip;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Remove all stoptimes outside the study zone,
 * then remove all trips with no stoptimes,
 * then remove all stops with no stoptimes,
 * then remove all transfers with no stop.
 */
public class CropGTFS {

    private static final Logger LOG = LoggerFactory.getLogger(CropGTFS.class);

    public static void main (String[] args) {

        if (args.length != 6) {
            System.out.printf("CropGTFS infile outfile lat0 lon0 lat1 lon1");
            System.out.printf("Example:");
            System.out.printf("CropGTFS marseille-aix-gtfs.zip cropped-gtfs.zip 43.08 4.52 43.95 5.80");
        }
        String inputFile = args[0];
        String outputFile = args[1];
        double lat0 = Double.parseDouble(args[2]);
        double lon0 = Double.parseDouble(args[3]);
        double lat1 = Double.parseDouble(args[4]);
        double lon1 = Double.parseDouble(args[5]);

        GTFSFeed feed = GTFSFeed.fromFile(inputFile);
        Set<String> retainedTripIds = new HashSet<>();

        Envelope envelope = new Envelope(lon0, lon1, lat0, lat1);

        System.out.println("Removing stops outside bounding box...");
        Iterator<Stop> stopIterator = feed.stops.values().iterator();
        while (stopIterator.hasNext()) {
            Stop stop = stopIterator.next();
            if ( ! envelope.contains(new Coordinate(stop.stop_lon, stop.stop_lat))) {
                stopIterator.remove();
            }
        }

        System.out.println("Removing stoptimes outside bounding box...");
        Iterator<StopTime> stIterator = feed.stop_times.values().iterator();
        while (stIterator.hasNext()) {
            StopTime stopTime = stIterator.next();
            if (feed.stops.containsKey(stopTime.stop_id)) {
                retainedTripIds.add(stopTime.trip_id);
            } else {
                stIterator.remove();
            }
        }

        System.out.println("Removing trips that had no stoptimes inside bounding box...");
        Iterator<Trip> tripIterator = feed.trips.values().iterator();
        while (tripIterator.hasNext()) {
            Trip trip = tripIterator.next();
            if ( ! retainedTripIds.contains(trip.trip_id)) {
                tripIterator.remove();
            }
        }

        System.out.println("Filtering transfers for removed stops...");
        Iterator<Transfer> ti = feed.transfers.values().iterator();
        while (ti.hasNext()) {
            Transfer t = ti.next();
            if ( ! (feed.stops.containsKey(t.from_stop.stop_id) && feed.stops.containsKey(t.to_stop.stop_id))) {
                ti.remove();
            }
        }

        System.out.println("Writing GTFS...");
        feed.toFile(outputFile);
        feed.db.close();
    }

}
