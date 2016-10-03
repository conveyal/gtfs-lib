package com.conveyal.gtfs.stats;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.model.CalendarDate;
import com.conveyal.gtfs.model.Pattern;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.Service;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.StreamSupport;

/**
 * Created by landon on 9/2/16.
 */
public class RouteStats {
    private GTFSFeed feed = null;
    private FeedStats stats = null;
    private PatternStats patternStats = null;
    private StopStats stopStats = null;

    public RouteStats (GTFSFeed f) {
        feed = f;
        stats = new FeedStats(feed);
        patternStats = new PatternStats(feed);
        stopStats = new StopStats(feed);
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
        List<Trip> tripsForRouteDirection = getTripsForDate(route_id, date).stream()
                .filter(t -> t.direction_id == direction_id)
                .collect(Collectors.toList());

        return patternStats.getAverageSpeedForTrips(tripsForRouteDirection, from, to);
    }

    /** Get the average headway of a route over a time window, in seconds */
    public int getHeadwayForRouteDirection (String route_id, int direction_id, LocalDate date, LocalTime from, LocalTime to) {
        Set<String> commonStops = null;

        List<Trip> tripsForRouteDirection = getTripsForDate(route_id, date).stream()
                .filter(t -> t.direction_id == direction_id)
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

        return stopStats.getStopHeadwayForTrips(commonStop, tripsForRouteDirection, from, to);
    }

    public LocalTime getStartTimeForRouteDirection (String route_id, int direction_id, LocalDate date) {


        List<Trip> tripsForRouteDirection = getTripsForDate(route_id, date).stream()
                .filter(t -> t.direction_id == direction_id)
                .collect(Collectors.toList());

        return patternStats.getStartTimeForTrips(tripsForRouteDirection);
    }

    public LocalTime getEndTimeForRouteDirection (String route_id, int direction_id, LocalDate date) {
        List<Trip> tripsForRouteDirection = getTripsForDate(route_id, date).stream()
                .filter(t -> t.direction_id == direction_id)
                .collect(Collectors.toList());

        return patternStats.getEndTimeForTrips(tripsForRouteDirection);
    }

    public Map<LocalDate, Integer> getTripCountPerDateOfService(String route_id) {

        Route route = feed.routes.get(route_id);
        if (route == null) return null;

        Map<LocalDate, List<Trip>> tripsPerDate = getTripsPerDateOfService(route_id);
        Map<LocalDate, Integer> tripCountPerDate = new HashMap<>();
        for (Map.Entry<LocalDate, List<Trip>> entry : tripsPerDate.entrySet()) {
            LocalDate date = entry.getKey();
            Integer count = entry.getValue().size();
            tripCountPerDate.put(date, count);
        }

        return tripCountPerDate;
    }

    public Map<LocalDate, List<Trip>> getTripsPerDateOfService(String route_id) {

        Map<String, List<Trip>> tripsPerService = getTripsPerService(route_id);
        Map<LocalDate, List<Trip>> tripsPerDate = new HashMap<>();


        LocalDate startDate = stats.getStartDate();
        LocalDate endDate = stats.getEndDate();

        // loop through services
        for (Service service : feed.services.values()) {
            // iterate through each date between start and end date
            for (LocalDate date = startDate; date.isBefore(endDate); date = date.plusDays(1)) {
                List<Trip> tripList = getTripsForDate(route_id, date);
                if (tripList == null) {
                    tripList = new ArrayList<>();
                }
                // if service is active on given day, add all trips that operate under that service
                if (service.activeOn(date)) {
                    List<Trip> serviceTrips = tripsPerService.get(service.service_id);
                    if (serviceTrips != null)
                        tripList.addAll(serviceTrips);

                }
                tripsPerDate.put(date, tripList);
            }
        }
        return tripsPerDate;
    }

    public Map<String, List<Trip>> getTripsPerService (String route_id) {
        Route route = feed.routes.get(route_id);
        if (route == null) return null;

        Map<String, List<Trip>> tripsPerService = stats.getTripsPerService();

        tripsPerService.forEach(((s, trips) -> {
            for (Iterator<Trip> it = trips.iterator(); it.hasNext();) {
                Trip trip = it.next();
                if (!trip.route_id.equals(route_id)) {
                    it.remove();
                }
            }
        }));
        return tripsPerService;
    }

    public List<Trip> getTripsForDate (String route_id, LocalDate date) {
        Route route = feed.routes.get(route_id);
        if (route == null) return null;

        List<Trip> trips = stats.getTripsForDate(date).stream()
                .filter(trip -> route_id.equals(trip.route_id))
                .collect(Collectors.toList());
        return trips;
    }

    public RouteStatistic getStatisticForRoute (String routeId) {
        RouteStatistic rs = new RouteStatistic();
        Route route = feed.routes.get(routeId);

        if (route == null) {
            throw new NullPointerException("Route does not exist.");
        }
//        feed.patterns.values().stream().filter(pattern -> pattern.route_id.equals(routeId)).forEach(p -> {
//            p.associatedTrips.forEach(t -> {
//                Trip trip = feed.trips.get(t);
//            });
//        });
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
