package com.conveyal.gtfs.stats;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.model.Pattern;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import com.conveyal.gtfs.stats.model.RouteStatistic;
import com.conveyal.gtfs.validator.service.GeoUtils;
import gnu.trove.list.TDoubleList;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;
import org.geotools.geometry.jts.JTS;
import org.mapdb.Fun;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.StreamSupport;

/**
 * Created by landon on 9/2/16.
 */
public class RouteStats {
    private GTFSFeed feed = null;
    private FeedStats stats = null;
    public RouteStats (GTFSFeed f) {
        feed = f;
        stats = new FeedStats(feed);
//        stats.getTripsPerDateOfService();
    }

    public List<RouteStatistic> getStatisticForAll () {
        List<RouteStatistic> stats = new ArrayList<>();

        for (String id : feed.routes.keySet()) {
            stats.add(getStatisticForRoute(id));
        }
        return stats;
    }

    public String getStatisticForAllAsCsv () {
        List<RouteStatistic> stats = getStatisticForAll();
        StringBuffer buffer = new StringBuffer();
        buffer.append(RouteStatistic.getHeaderAsCsv());
        for (RouteStatistic rs : stats) {
            buffer.append(System.getProperty("line.separator"));
            buffer.append(rs.asCsv());
        }

        return buffer.toString();
    }

    /** Get average speed on a direction of a route, in meters/second */
    public double getSpeedForRouteDirection (String route_id, int direction_id, LocalDate date, LocalTime from, LocalTime to) {
        System.out.println(route_id);
        int count = 0;
        List<Trip> trips = stats.getTripsForDate(date);
        TDoubleList speeds = new TDoubleArrayList();
        long first = 86399;
        long last = 0;

        for (Trip trip : trips.stream().filter(t -> t.route_id.equals(route_id) && t.direction_id == direction_id).collect(Collectors.toList())) {

            StopTime firstStopTime = feed.stop_times.ceilingEntry(Fun.t2(trip.trip_id, null)).getValue();
            StopTime lastStopTime = feed.stop_times.floorEntry(Fun.t2(trip.trip_id, Fun.HI)).getValue();

            // ensure that stopTime returned matches trip id (i.e., that the trip has stoptimes)
            if (!firstStopTime.trip_id.equals(trip.trip_id) || !lastStopTime.trip_id.equals(trip.trip_id)) {
                continue;
            }

            LocalTime tripBeginTime = LocalTime.ofSecondOfDay(firstStopTime.departure_time % 86399); // convert 24hr+ seconds to 0 - 86399
            if (tripBeginTime.isAfter(to) || tripBeginTime.isBefore(from)) {
                continue;
            }
            count++;
            if (firstStopTime.departure_time <= first) {
                first = firstStopTime.departure_time;
            }
            if (firstStopTime.departure_time >= last) {
                last = firstStopTime.departure_time;
            }
            // TODO: Matt, work your geography magic.
            double distance = GeoUtils.getDistance(feed.getStraightLineForStops(trip.trip_id));
            int time = lastStopTime.arrival_time - firstStopTime.departure_time;
            if (time != 0)
                speeds.add(distance / time); // meters per second
        }

        if (speeds.isEmpty()) return Double.NaN;

        return speeds.sum() / speeds.size();
    }

    /** Get the average headway of a route over a time window, in seconds */
    public int getHeadwayForRouteDirection (String route_id, int direction_id, LocalDate date, LocalTime from, LocalTime to) {
        System.out.println(route_id);
        List<Trip> trips = stats.getTripsForDate(date);

        TIntList timesAtStop = new TIntArrayList();

        Set<String> commonStops = null;

        List<Trip> tripsForRouteDirection = trips.stream()
                 .filter(t -> t.route_id.equals(route_id) && t.direction_id == direction_id)
                .collect(Collectors.toList());

        for (Trip trip : tripsForRouteDirection) {
            List<String> stops = feed.getOrderedStopListForTrip(trip.trip_id);

            if (commonStops == null) {
                commonStops = new HashSet<>(stops);
            } else {
                commonStops.retainAll(stops);
            }
        }

        if (commonStops.isEmpty()) return -1;

        String commonStop = commonStops.iterator().next();

        for (Trip trip : tripsForRouteDirection) {
            StopTime st;
            try {
                // use interpolated times in case our common stop is not a time point
                st = StreamSupport.stream(feed.getInterpolatedStopTimesForTrip(trip.trip_id).spliterator(), false)
                        .filter(candidate -> candidate.stop_id.equals(commonStop))
                        .findFirst()
                        .orElse(null);
            } catch (GTFSFeed.FirstAndLastStopsDoNotHaveTimes e) {
                return -1;
            }

            // these trips are actually running on the next day, skip them
            if (st.departure_time > 86399) continue;

            LocalTime timeAtStop = LocalTime.ofSecondOfDay(st.departure_time);

            if (timeAtStop.isAfter(to) || timeAtStop.isBefore(from)) {
                continue;
            }

            timesAtStop.add(st.departure_time);
        }

        timesAtStop.sort();

        // convert to deltas
        TIntList deltas = new TIntArrayList();

        for (int i = 0; i < timesAtStop.size() - 1; i++) {
            int delta = timesAtStop.get(i + 1) - timesAtStop.get(i);

            if (delta > 60) deltas.add(delta);
        }

        if (deltas.isEmpty()) return -1;

        return deltas.sum() / deltas.size();
    }

    public RouteStatistic getStatisticForRoute (String routeId) {
        RouteStatistic rs = new RouteStatistic();
        Route route = feed.routes.get(routeId);

        if (route == null) {
            throw new NullPointerException("Route does not exist.");
        }
        feed.patterns.values().stream().filter(pattern -> pattern.route_id == routeId).forEach(p -> {
            p.associatedTrips.forEach(t -> {
                Trip trip = feed.trips.get(t);
            });
        });
        rs.routeId = route.route_id;
        rs.routeName = route.route_short_name != null ? route.route_short_name : route.route_long_name;
        rs.headwayPeak = null;
        rs.headwayOffPeak = null;
        rs.avgSpeedPeak = null;
        rs.avgSpeedOffPeak = null;
        return rs;
    }

    public String getKCMetroStatistics () {
        for (Route route : feed.routes.values()) {

        }
        return null;
    }

    public String getStatisticsAsCsv () {
        return null;
    }
}
