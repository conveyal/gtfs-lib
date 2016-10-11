package com.conveyal.gtfs.stats;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.model.Pattern;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import gnu.trove.list.TDoubleList;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;
import org.mapdb.Fun;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Collection;
import java.util.List;
import java.util.Spliterator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Created by landon on 9/2/16.
 */
public class PatternStats {

    private GTFSFeed feed = null;
    private FeedStats stats = null;

    public PatternStats (GTFSFeed f, FeedStats fs) {
        feed = f;
        stats = fs;
    }

    /**
     * Gets the pattern speed for a given pattern for a specified date and time window.
     * @param pattern_id
     * @param date
     * @param from
     * @param to
     * @return
     */
    public double getPatternSpeed (String pattern_id, LocalDate date, LocalTime from, LocalTime to) {

        List<Trip> trips = getTripsForDate(pattern_id, date);

        return getAverageSpeedForTrips(trips, from, to);
    }

    /**
     * Get average speed for set of trips that begin within the time window in meters per second.
     * @param trips
     * @param from
     * @param to
     * @return avg. speed (meters per second)
     */
    public double getAverageSpeedForTrips (Collection<Trip> trips, LocalTime from, LocalTime to) {
        TDoubleList speeds = new TDoubleArrayList();

        for (Trip trip : trips) {
            StopTime firstStopTime = feed.stop_times.ceilingEntry(Fun.t2(trip.trip_id, null)).getValue();
            LocalTime tripBeginTime = LocalTime.ofSecondOfDay(firstStopTime.departure_time % 86399); // convert 24hr+ seconds to 0 - 86399

            // skip trip if begin time is before or after specified time period
            if (tripBeginTime.isAfter(to) || tripBeginTime.isBefore(from)) {
                continue;
            }
            // TODO: swap straight lines for actual geometry?
            double speed = feed.getTripSpeed(trip.trip_id, true);

            if (!Double.isNaN(speed)) {
                speeds.add(speed);
            }
        }

        if (speeds.isEmpty()) return -1;

        return speeds.sum() / speeds.size();
    }

    /**
     * Get earliest departure time for a set of trips.
     * @param trips
     * @return earliest departure time
     */
    public LocalTime getStartTimeForTrips (Collection<Trip> trips) {
        int earliestDeparture = Integer.MAX_VALUE;

        for (Trip trip : trips) {
            StopTime st = feed.getOrderedStopTimesForTrip(trip.trip_id).iterator().next();
            int dep = st.departure_time;

            // these trips begin on the next day, so we need to cast them to 0 - 86399
            if (dep > 86399) {
                dep = dep % 86399;
            }

            if (dep <= earliestDeparture) {
                earliestDeparture = dep;
            }
        }
        return LocalTime.ofSecondOfDay(earliestDeparture);
    }

    /**
     * Get last arrival time for a set of trips.
     * @param trips
     * @return last arrival time (if arrival occurs after midnight, time is expressed in terms of following day, e.g., 2:00 AM)
     */
    public LocalTime getEndTimeForTrips (Collection<Trip> trips) {
        int latestArrival = Integer.MIN_VALUE;

        for (Trip trip : trips) {
            StopTime st = feed.getOrderedStopTimesForTrip(trip.trip_id).iterator().next();

            if (st.arrival_time >= latestArrival) {
                latestArrival = st.arrival_time;
            }
        }

        // return end time as 2:00 am if last arrival occurs after midnight
        return LocalTime.ofSecondOfDay(latestArrival % 86399);
    }

    /**
     * Get total revenue time (in seconds) for set of trips.
     * @param trips
     * @return total revenue time (in seconds)
     */
    public int getTotalRevenueTimeForTrips (Collection<Trip> trips) {
        TIntList times = new TIntArrayList();
        for (Trip trip : trips) {
            StopTime first;
            StopTime last;
            Spliterator<StopTime> stopTimes = feed.getOrderedStopTimesForTrip(trip.trip_id).spliterator();;

            first = StreamSupport.stream(stopTimes, false)
                    .findFirst()
                    .orElse(null);

            last = StreamSupport.stream(stopTimes, false)
                    .reduce((a, b) -> b)
                    .orElse(null);

            // revenue time should not include layovers at termini
            int time = last.arrival_time - first.departure_time;

            times.add(time);
        }

        return times.sum();
    }

    /**
     * Get total revenue distance (in meters) for set of trips.
     * @param trips
     * @return total trip distance (in meters)
     */
    public double getTotalDistanceForTrips (Collection<Trip> trips) {
        TDoubleList distances = new TDoubleArrayList();
        for (Trip trip : trips) {
            distances.add(feed.getTripDistance(trip.trip_id, false));
        }

        return distances.sum();
    }

    /**
     * Get distance for a pattern. Uses the first trip associated with the pattern.
     * @param pattern_id
     * @return distance (in meters)
     */
    public double getPatternDistance (String pattern_id) {
        Pattern pattern = feed.patterns.get(pattern_id);

        return feed.getTripDistance(pattern.associatedTrips.iterator().next(), false);
    }

    /**
     * Get average stop spacing for a pattern.
     * @param pattern_id
     * @return avg. stop spacing (in meters)
     */
    public double getAverageStopSpacing (String pattern_id) {
        Pattern pattern = feed.patterns.get(pattern_id);

        return getPatternDistance(pattern_id) / pattern.orderedStops.size();
    }


    public int getHeadwayForPattern (String pattern_id, LocalDate date, LocalTime from, LocalTime to) {
        
        List<Trip> tripsForPattern = getTripsForDate(pattern_id, date);

        String commonStop = stats.route.getCommonStopForTrips(tripsForPattern);
        if (commonStop == null) return -1;

        return stats.stop.getStopHeadwayForTrips(commonStop, tripsForPattern, from, to);
    }

    public long getTripCountForDate (String pattern_id, LocalDate date) {
        return getTripsForDate(pattern_id, date).size();
    }

    public List<Trip> getTripsForDate (String pattern_id, LocalDate date) {
        Pattern pattern = feed.patterns.get(pattern_id);
        if (pattern == null) return null;

        List<Trip> trips = stats.getTripsForDate(date).stream()
                .filter(trip -> pattern.associatedTrips.contains(trip.trip_id))
                .collect(Collectors.toList());

        return trips;
    }
}
