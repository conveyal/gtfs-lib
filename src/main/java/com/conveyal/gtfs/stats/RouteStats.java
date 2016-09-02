package com.conveyal.gtfs.stats;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.model.Pattern;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import com.conveyal.gtfs.stats.model.RouteStatistic;
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

    public Double getSpeedForRouteDirection (String route_id, int direction_id, LocalDate date, LocalTime from, LocalTime to) {
        System.out.println(route_id);
        int count = 0;
        List<Trip> trips = stats.getTripsForDate(date);
        List<Double> speeds = new ArrayList<>();
        long first = 86399;
        long last = 0;

        for (Trip trip : trips.stream().filter(t -> t.route_id == route_id && t.direction_id == direction_id).collect(Collectors.toList())) {

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
            Double distance = feed.getTripGeometry(trip.trip_id).getLength(); // JTS.orthodromicDistance(feed.getTripGeometry(trip.trip_id));
            int time = lastStopTime.arrival_time - firstStopTime.departure_time;
            if (time != 0 && distance != null)
                speeds.add(distance / time); // meters per second
        }

        return speeds.stream().mapToDouble(a -> a).average().getAsDouble();
    }

    public Double getFrequencyForRouteDirection (String route_id, int direction_id, LocalDate date, LocalTime from, LocalTime to) {
        System.out.println(route_id);
        int count = 0;
        List<Trip> trips = stats.getTripsForDate(date);
        long first = 86399;
        long last = 0;

        for (Trip trip : trips.stream().filter(t -> t.route_id == route_id && t.direction_id == direction_id).collect(Collectors.toList())) {

            StopTime st = feed.stop_times.ceilingEntry(Fun.t2(trip.trip_id, null)).getValue();
            LocalTime tripBeginTime = LocalTime.ofSecondOfDay(st.departure_time % 86399); // convert 24hr+ seconds to 0 - 86399


            if (tripBeginTime.isAfter(to) || tripBeginTime.isBefore(from)) {
                continue;
            }
            count++;
            if (st.departure_time <= first) {
                first = st.departure_time;
            }
            if (st.departure_time >= last) {
                last = st.departure_time;
            }
        }

        if (count == 0) {
            return 0.0;
        }

        Double frequency = Double.valueOf( (last - first) / count );

        System.out.println(frequency / 60);
        return frequency;
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
