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
 * Remove all stops outside the bounding box,
 * then remove all stop_times outside the bounding box,
 * recording all trips with two or more stop_times inside the bounding box.
 * Then remove all trips with no stoptimes or one single stoptime,
 * then remove all transfers whose stops have been removed.
 *
 * Note that this does not crop the GTFS shapes, only the stops and stoptimes.
 * Therefore in some tools like Transport Analyst, the data set will appear to extend beyond the bounding box
 * because the entire shapes are drawn.
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

        // We keep two sets of trip IDs because we only keep trips that are referenced by two or more stopTimes.
        // A TObjectIntMap would be good for this as well, but we don't currently depend on Trove.
        Set<String> referencedTripIds = new HashSet<>();
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

        System.out.println("Removing stop_times outside the bounding box and finding trips with two or more stop_times inside the bounding box...");
        Iterator<StopTime> stIterator = feed.stop_times.values().iterator();
        while (stIterator.hasNext()) {
            StopTime stopTime = stIterator.next();
            if (feed.stops.containsKey(stopTime.stop_id)) {
                // This stop has been retained because it's inside the bounding box.
                // Keep the stop_time, and also record the trip_id it belongs to so we can retain those.
                if (referencedTripIds.contains(stopTime.trip_id)) {
                    // This trip is referenced by two or more stopTimes within the bounding box.
                    retainedTripIds.add(stopTime.trip_id);
                } else {
                    // This is the first time this trip has been referenced by a stopTime within the bounding box.
                    referencedTripIds.add(stopTime.trip_id);
                }
            } else {
                // Skip stops outside the bounding box, but keep those within the bounding box.
                // It is important to remove these or we'll end up with trips referencing stop IDs that don't exist.
                stIterator.remove();
            }
        }

        System.out.println("Removing stoptimes for trips with less than two stop_times inside the bounding box...");
        // There are more efficient ways of doing this than iterating over all the stop times.
        // It could be done inside the trip removal loop below.
        stIterator = feed.stop_times.values().iterator();
        while (stIterator.hasNext()) {
            StopTime stopTime = stIterator.next();
            if ( ! retainedTripIds.contains(stopTime.trip_id)) {
                // This stop_time's trip is not referenced by two or more stopTimes within the bounding box.
                stIterator.remove();
            }
        }

        System.out.println("Removing trips that had less than two stop_times inside bounding box...");
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
            if ( ! (feed.stops.containsKey(t.from_stop_id) && feed.stops.containsKey(t.to_stop_id))) {
                ti.remove();
            }
        }

        System.out.println("Writing GTFS...");
        feed.toFile(outputFile);
        feed.close();
    }

}
