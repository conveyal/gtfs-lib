package com.conveyal.gtfs.stats;

import com.conveyal.gtfs.GTFSFeed;
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
        List<Trip> tripsForRouteDirection = getTripsForDate(route_id, date).stream()
                .filter(t -> t.direction_id == direction_id)
                .collect(Collectors.toList());

        TDoubleList speeds = new TDoubleArrayList();
        long first = 86399;
        long last = 0;

        for (Trip trip : tripsForRouteDirection) {

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

    public LocalTime getStartTimeForRouteDirection (String route_id, int direction_id, LocalDate date) {
        int earliestDeparture = Integer.MAX_VALUE;

        List<Trip> tripsForRouteDirection = getTripsForDate(route_id, date).stream()
                .filter(t -> t.direction_id == direction_id)
                .collect(Collectors.toList());

        for (Trip trip : tripsForRouteDirection) {
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

    public LocalTime getEndTimeForRouteDirection (String route_id, int direction_id, LocalDate date) {
        int latestArrival = Integer.MIN_VALUE;

        List<Trip> tripsForRouteDirection = getTripsForDate(route_id, date).stream()
                .filter(t -> t.direction_id == direction_id)
                .collect(Collectors.toList());

        for (Trip trip : tripsForRouteDirection) {
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

            // skip service if start or end date is null
            if (startDate == null || endDate == null) {
                continue;
            }

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
            for (Trip trip : trips) {
                if (!route_id.equals(trip.route_id)) {
                    trips.remove(trip);
                }
            }
        }));
        return tripsPerService;
    }

    public List<Trip> getTripsForDate (String route_id, LocalDate date) {
        Route route = feed.routes.get(route_id);
        if (route == null) return null;

        List<Trip> trips = stats.getTripsForDate(date).stream().filter(trip -> route_id.equals(trip.route_id)).collect(Collectors.toList());
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
