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

    public double getPatternSpeed (String pattern_id, LocalDate date, LocalTime from, LocalTime to) {

        List<Trip> trips = getTripsForDate(pattern_id, date);

        return getAverageSpeedForTrips(trips, from, to);
    }

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

        if (speeds.isEmpty()) return Double.NaN;

        return speeds.sum() / speeds.size();
    }

    public LocalTime getStartTimeForTrips (Collection<Trip> trips) {
        int earliestDeparture = Integer.MAX_VALUE;

        for (Trip trip : trips) {
            StopTime st;
            try {
                // use interpolated times in case our common stop is not a time point
                st = StreamSupport.stream(feed.getInterpolatedStopTimesForTrip(trip.trip_id).spliterator(), false)
                        .findFirst()
                        .orElse(null);
            } catch (GTFSFeed.FirstAndLastStopsDoNotHaveTimes e) {
                return null;
            }

            // shouldn't actually encounter any departures after midnight, but skip any departures that do
            if (st.departure_time > 86399) continue;

            if (st.departure_time <= earliestDeparture) {
                earliestDeparture = st.departure_time;
            }
        }
        return LocalTime.ofSecondOfDay(earliestDeparture);
    }

    public LocalTime getEndTimeForTrips (Collection<Trip> trips) {
        int latestArrival = Integer.MIN_VALUE;

        for (Trip trip : trips) {
            StopTime st;
            try {
                // use interpolated times in case our common stop is not a time point
                st = StreamSupport.stream(feed.getInterpolatedStopTimesForTrip(trip.trip_id).spliterator(), false)
                        .reduce((a, b) -> b)
                        .orElse(null);
            } catch (GTFSFeed.FirstAndLastStopsDoNotHaveTimes e) {
                return null;
            }

            if (st.arrival_time >= latestArrival) {
                latestArrival = st.arrival_time;
            }
        }

        // return end time as 2:00 am if last arrival occurs after midnight
        return LocalTime.ofSecondOfDay(latestArrival % 86399);
    }

    /** Get total revenue time (in seconds) for set of trips. */
    public int getTotalRevenueTimeForTrips (Collection<Trip> trips) {
        TIntList times = new TIntArrayList();
        for (Trip trip : trips) {
            StopTime first;
            StopTime last;
            Spliterator<StopTime> interpolated = null;
            try {
                interpolated = feed.getInterpolatedStopTimesForTrip(trip.trip_id).spliterator();
            } catch (GTFSFeed.FirstAndLastStopsDoNotHaveTimes firstAndLastStopsDoNotHaveTimes) {
                return -1;
            }

            first = StreamSupport.stream(interpolated, false)
                    .findFirst()
                    .orElse(null);

            last = StreamSupport.stream(interpolated, false)
                    .reduce((a, b) -> b)
                    .orElse(null);
            int time = last.departure_time - first.arrival_time;

            times.add(time);
        }

        return times.sum();
    }

    /** Get total revenue distance (in meters) for set of trips. */
    public double getTotalRevenueDistanceForTrips (Collection<Trip> trips) {
        TDoubleList distances = new TDoubleArrayList();
        for (Trip trip : trips) {
            distances.add(feed.getTripDistance(trip.trip_id, false));
        }

        return distances.sum();
    }

    public double getPatternDistance (String pattern_id) {
        Pattern pattern = feed.patterns.get(pattern_id);

        return feed.getTripDistance(pattern.associatedTrips.iterator().next(), false);
    }

    public double getAverageStopSpacing (String pattern_id) {
        Pattern pattern = feed.patterns.get(pattern_id);

        return getPatternDistance(pattern_id) / pattern.orderedStops.size();
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
