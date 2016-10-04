package com.conveyal.gtfs.stats;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.Service;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import com.conveyal.gtfs.stats.model.TransferPerformanceSummary;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import org.mapdb.Fun;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Created by landon on 9/2/16.
 */
public class StopStats {

    private GTFSFeed feed = null;
    private FeedStats stats = null;
    private RouteStats routeStats = null;

    public StopStats (GTFSFeed f, FeedStats fs) {
        feed = f;
        stats = fs;
        routeStats = stats.route;
    }

    public Set<Route> getRoutes (String stop_id) {
        return feed.patterns.values().stream()
                .filter(p -> p.orderedStops.contains(stop_id))
                .map(p -> feed.routes.get(p.route_id))
                .collect(Collectors.toSet());
    }

    public int getRouteCount (String stop_id) {
        return getRoutes(stop_id).size();
    }

    public int getTripCountForDate (String stop_id, LocalDate date) {
        return (int) getTripsForDate(stop_id, date).size();
    }

    /** Get list of trips for specified date of service */
    public List<Trip> getTripsForDate (String stop_id, LocalDate date) {
        List<String> tripIds = stats.getTripsForDate(date).stream()
                .map(trip -> trip.trip_id)
                .collect(Collectors.toList());

        return feed.getStopTimesForStop(stop_id).stream()
                .map(t -> feed.trips.get(t.b.a)) // map to trip ids
                .filter(t -> tripIds.contains(t.trip_id)) // filter by trip_id list for date
                .collect(Collectors.toList());
    }

    public int getAverageHeadwayForStop (String stop_id, LocalDate date, LocalTime from, LocalTime to) {
        List<Trip> tripsForStop = feed.getStopTimesForStop(stop_id).stream()
                .map(t -> feed.trips.get(t.b.a))
                .filter(trip -> feed.services.get(trip.service_id).activeOn(date))
                .collect(Collectors.toList());

        return getStopHeadwayForTrips(stop_id, tripsForStop, from, to);
    }

    /** Get the route headway for a given service date at a stop over a time window, in seconds */
    public Map<String, Integer> getRouteHeadwaysForStop (String stop_id, LocalDate date, LocalTime from, LocalTime to) {
        Map<String, Integer> routeHeadwayMap = new HashMap<>();
        List<Route> routes = feed.patterns.values().stream()
                .filter(p -> p.orderedStops.contains(stop_id))
                .map(p -> feed.routes.get(p.route_id))
                .collect(Collectors.toList());

        for (Route route : routes) {
            routeHeadwayMap.put(route.route_id, getHeadwayForStopByRoute(stop_id, route.route_id, date, from, to));
        }
        return routeHeadwayMap;
    }

    /** Get the average headway for a set of trips at a stop over a time window, in seconds */
    public int getStopHeadwayForTrips (String stop_id, List<Trip> trips, LocalTime from, LocalTime to) {
        TIntList timesAtStop = new TIntArrayList();

        for (Trip trip : trips) {
            StopTime st;
            try {
                // use interpolated times in case our common stop is not a time point
                st = StreamSupport.stream(feed.getInterpolatedStopTimesForTrip(trip.trip_id).spliterator(), false)
                        .filter(candidate -> candidate.stop_id.equals(stop_id))
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

    public int getHeadwayForStopByRoute (String stop_id, String route_id, LocalDate date, LocalTime from, LocalTime to) {

        List<Trip> tripsForStop = feed.getStopTimesForStop(stop_id).stream()
                .filter(t -> feed.trips.get(t.a).route_id.equals(route_id))
                .map(t -> feed.trips.get(t.b.a))
                .filter(trip -> feed.services.get(trip.service_id).activeOn(date))
                .collect(Collectors.toList());

        return getStopHeadwayForTrips(stop_id, tripsForStop, from, to);
    }

    /** Returns a map from route_id to transferPerformanceSummary for each route at a stop for a specified date. */
    public Map<String, List<TransferPerformanceSummary>> getTransferPerformance (String stop_id, LocalDate date) {
        SortedSet<Fun.Tuple2<String, Fun.Tuple2>> stopTimes = feed.getStopTimesForStop(stop_id);
        Map<Route, List<StopTime>> routeStopTimeMap  = new HashMap<>();
        Map<String, List<TransferPerformanceSummary>> transferPerformanceMap = new HashMap<>();
        // TODO: do we need to handle interpolated stop times???

        // first stream stopTimes for stop into route -> list of stopTimes map
        stopTimes.stream()
                .forEach(t -> {
                    Trip trip = feed.trips.get(t.b.a);
                    Service service = feed.services.get(trip.service_id);
                    // only add to map if trip is active on given date
                    if (service != null && service.activeOn(date)) {
                        StopTime st = feed.stop_times.get(t.b);
                        Route route = feed.routes.get(trip.route_id);

                        List<StopTime> times = new ArrayList<>();
                        if (routeStopTimeMap.containsKey(route)) {
                            times.addAll(routeStopTimeMap.get(route));
                        }
                        times.add(st);
                        routeStopTimeMap.put(route, times);
                    }
                });

        // iterate over every entry in route -> list of stopTimes map
        for (Map.Entry<Route, List<StopTime>> entry : routeStopTimeMap.entrySet()) {
            final int MISSED_TRANSFER_THRESHOLD = 60 * 10;
            List<StopTime> currentTimes = entry.getValue();
            Set<Set<StopTime>> missedTransfers = new HashSet<>();
            Route currentRoute = entry.getKey();
            for (StopTime currentTime : currentTimes) {
                if (currentTime.arrival_time > 0) {

                    // cycle through all other routes that stop here.
                    for (Map.Entry<Route, List<StopTime>> entry2 : routeStopTimeMap.entrySet()) {
                        TIntList waitTimes = new TIntArrayList();
                        List<StopTime> compareTimes = entry2.getValue();
                        Route compareRoute = entry2.getKey();
                        if (compareRoute.route_id.equals(currentRoute.route_id)) {
                            continue;
                        }
                        // compare current time against departure times for route
                        for (StopTime compareTime : compareTimes) {
                            if (compareTime.departure_time > 0) {
                                int waitTime = compareTime.departure_time - currentTime.arrival_time;

                                // if non-negative wait time, add to list
                                if (waitTime >= 0){
                                    waitTimes.add(waitTime);
                                }
                                // otherwise, check for missed opportunities
                                else {
                                    if (waitTime * -1 <= MISSED_TRANSFER_THRESHOLD) {
                                        Set<StopTime> missedTransfer = new HashSet<>();
                                        missedTransfer.add(currentTime);
                                        missedTransfer.add(compareTime);
                                        missedTransfers.add(missedTransfer);
                                    }
                                }
                            }
                        }
                        if (waitTimes.isEmpty()) {
                            continue;
                        }
                        int min = waitTimes.min();
                        int max = waitTimes.max();
                        int avg = waitTimes.sum() / waitTimes.size();
                        TransferPerformanceSummary routeTransferPerformance = new TransferPerformanceSummary(currentRoute.route_id, compareRoute.route_id, min, max, avg, missedTransfers);
                        List<TransferPerformanceSummary> tpList = new ArrayList<>();
                        if (transferPerformanceMap.containsKey(currentRoute.route_id)) {
                            tpList.addAll(transferPerformanceMap.get(currentRoute.route_id));
                        }
                        tpList.add(routeTransferPerformance);
                        transferPerformanceMap.put(currentRoute.route_id, tpList);
                    }
                }
            }
        }
        return transferPerformanceMap;
    }
}
