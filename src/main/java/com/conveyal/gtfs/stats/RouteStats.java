package com.conveyal.gtfs.stats;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.Service;
import com.conveyal.gtfs.model.Trip;
import com.conveyal.gtfs.stats.model.RouteStatistic;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
    private PatternStats patternStats = null;
    private StopStats stopStats = null;

    public RouteStats (GTFSFeed f, FeedStats fs) {
        feed = f;
        stats = fs;
        patternStats = stats.pattern;
        stopStats = stats.stop;
    }

    public List<RouteStatistic> getStatisticForAll (LocalDate date, LocalTime from, LocalTime to) {
        List<RouteStatistic> stats = new ArrayList<>();

        for (String id : feed.routes.keySet()) {
            stats.add(getStatisticForRoute(id, date, from, to));
        }
        return stats;
    }

    public String getStatisticForAllAsCsv (LocalDate date, LocalTime from, LocalTime to) {
        List<RouteStatistic> stats = getStatisticForAll(date, from, to);
        StringBuffer buffer = new StringBuffer();
        buffer.append(RouteStatistic.getHeaderAsCsv());
        for (RouteStatistic rs : stats) {
            buffer.append(System.getProperty("line.separator"));
            buffer.append(rs.asCsv());
        }

        return buffer.toString();
    }

    public String getRouteName (String route_id) {
        Route route = feed.routes.get(route_id);
        return route != null ? route.route_short_name + " - " + route.route_long_name : null;
    }

    /**
     * Get average speed on a direction of a route, in meters per second.
     * @param route_id
     * @param direction_id
     * @param date
     * @param from
     * @param to
     * @return avg. speed (meters per second)
     */
    public double getSpeedForRouteDirection (String route_id, int direction_id, LocalDate date, LocalTime from, LocalTime to) {
        List<Trip> tripsForRouteDirection = getTripsForDate(route_id, date).stream()
                .filter(t -> t.direction_id == direction_id)
                .collect(Collectors.toList());

        return patternStats.getAverageSpeedForTrips(tripsForRouteDirection, from, to);
    }

    /**
     * Get the average headway for a route direction for a specified service date and time window, in seconds.
     * @param route_id
     * @param direction_id
     * @param date
     * @param from
     * @param to
     * @return avg. headway in seconds
     */
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

    /**
     * Get earliest departure time for a given route direction on the specified date.
     * @param route_id
     * @param direction_id
     * @param date
     * @return earliest departure time
     */
    public LocalTime getStartTimeForRouteDirection (String route_id, int direction_id, LocalDate date) {


        List<Trip> tripsForRouteDirection = getTripsForDate(route_id, date).stream()
                .filter(t -> t.direction_id == direction_id)
                .collect(Collectors.toList());

        return patternStats.getStartTimeForTrips(tripsForRouteDirection);
    }

    /**
     * Get latest arrival time for a given route direction on the specified date.
     * @param route_id
     * @param direction_id
     * @param date
     * @return last arrival time
     */
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

    /**
     * Returns a map of dates to list of trips for a given route.
     * @param route_id
     * @return mapping of trips to dates
     */
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

    /**
     * Maps a list of trips to all service entries for a given route.
     * @param route_id
     * @return mapping of trips to service_id
     */
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

    /**
     * Gets all trips for a given route that operate on the specified date.
     * @param route_id
     * @param date
     * @return
     */
    public List<Trip> getTripsForDate (String route_id, LocalDate date) {
        Route route = feed.routes.get(route_id);
        if (route == null) return null;

        List<Trip> trips = stats.getTripsForDate(date).stream()
                .filter(trip -> route_id.equals(trip.route_id))
                .collect(Collectors.toList());
        return trips;
    }

    public RouteStatistic getStatisticForRoute (String route_id, LocalDate date, LocalTime from, LocalTime to) {
        RouteStatistic rs = new RouteStatistic(this, route_id, date, from, to);

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
