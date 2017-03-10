package com.conveyal.gtfs.stats;

import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.model.Agency;
import com.conveyal.gtfs.model.Service;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import com.conveyal.gtfs.stats.model.AgencyStatistic;
import com.vividsolutions.jts.geom.Geometry;
import org.mapdb.Fun;

/**
 * Retrieves a base set of statistics from the GTFS.
 *
 */
public class FeedStats {

    private GTFSFeed feed = null;
    public String feed_id = null;
    public PatternStats pattern = null;
    public StopStats stop = null;
    public RouteStats route = null;

    public FeedStats(GTFSFeed f) {
        this.feed = f;
        this.feed_id = f.feedId;
        this.pattern = new PatternStats(feed, this);
        this.stop = new StopStats(feed, this);
        this.route = new RouteStats(feed, this);
    }

    public Integer getAgencyCount() {
        return feed.agency.size();
    }

    public Integer getRouteCount() {
        return feed.routes.size();
    }

    public Integer getTripCount() {
        return feed.trips.size();
    }

    public Integer getTripCount(LocalDate date) {
        return getTripsForDate(date).size();
    }


    public Integer getStopCount() {
        return feed.stops.size();
    }

    public Integer getStopTimesCount() {
        return feed.stop_times.size();
    }

    // calendar date range start/end assume a service calendar based schedule
    // returns null for schedules without calendar service schedules

    public LocalDate getCalendarServiceRangeStart() {

        int startDate = 0;
        for (Service service : feed.services.values()) {
            if (service.calendar == null)
                continue;
            if (startDate == 0 || service.calendar.start_date < startDate) {
                startDate = service.calendar.start_date;
            }
        }
        if (startDate == 0)
            return null;

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        return LocalDate.parse(String.valueOf(startDate), formatter);
    }

    public LocalDate getCalendarServiceRangeEnd() {

        int endDate = 0;

        for (Service service : feed.services.values()) {
            if (service.calendar == null)
                continue;

            if (endDate == 0 || service.calendar.end_date > endDate) {
                endDate = service.calendar.end_date;
            }
        }
        if (endDate == 0)
            return null;

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        return LocalDate.parse(String.valueOf(endDate), formatter);
    }

    public LocalDate getCalendarDateStart() {
        LocalDate startDate = null;
        for (Service service : feed.services.values()) {
            for (LocalDate date : service.calendar_dates.keySet()) {
                if (startDate == null
                        || date.isBefore(startDate))
                startDate = date;
            }
        }
        return startDate;
    }

    public LocalDate getCalendarDateEnd() {
        LocalDate endDate = null;
        for (Service service : feed.services.values()) {
            for (LocalDate date : service.calendar_dates.keySet()) {
                if (endDate == null
                        || date.isAfter(endDate))
                    endDate = date;
            }
        }
        return endDate;
    }

    public Map<LocalDate, Integer> getTripCountPerDateOfService() {

        Map<LocalDate, List<Trip>> tripsPerDate = getTripsPerDateOfService();
        Map<LocalDate, Integer> tripCountPerDate = new HashMap<>();
        for (Map.Entry<LocalDate, List<Trip>> entry : tripsPerDate.entrySet()) {
            LocalDate date = entry.getKey();
            Integer count = entry.getValue().size();
            tripCountPerDate.put(date, count);
        }

        return tripCountPerDate;
    }
    public LocalDate getStartDate() {
        LocalDate startDate = null;

        if (feed.hasFeedInfo()) startDate = feed.getFeedInfo().feed_start_date;
        if (startDate == null) startDate = getCalendarServiceRangeStart();
        if (startDate == null) startDate = getCalendarDateStart();

        return startDate;
    }

    public LocalDate getEndDate() {
        LocalDate endDate = null;

        if (feed.hasFeedInfo()) endDate = feed.getFeedInfo().feed_end_date;
        if (endDate == null) endDate = getCalendarServiceRangeEnd();
        if (endDate == null) endDate = getCalendarDateEnd();

        return endDate;
    }

    public LocalTime getStartTime (LocalDate date) {
        return this.pattern.getStartTimeForTrips(getTripsForDate(date));
    }

    public LocalTime getEndTime (LocalDate date) {
        return this.pattern.getEndTimeForTrips(getTripsForDate(date));
    }

    /** Get total revenue time (in seconds) for all trips on a given date. */
    public long getTotalRevenueTimeForDate (LocalDate date) {
        return this.pattern.getTotalRevenueTimeForTrips(getTripsForDate(date));
    }

    public long getAverageDailyRevenueTime (int dow) {
        // int value of dow from 1 (Mon) to 7 (Sun)
        DayOfWeek dayOfWeek = DayOfWeek.of(dow);
        List<LocalDate> dates = feed.getDatesOfService().stream()
                .filter(date -> date.getDayOfWeek().equals(dayOfWeek))
                .collect(Collectors.toList());

        return getRevenueTimeForDates(dates) / dates.size();
    }

    public long getAverageWeekdayRevenueTime () {
        List<LocalDate> dates = feed.getDatesOfService().stream()
                .filter(date -> {
                    int dow = date.getDayOfWeek().getValue();
                    boolean isWeekday = ((dow >= DayOfWeek.MONDAY.getValue()) && (dow <= DayOfWeek.FRIDAY.getValue()));
                    return isWeekday;
                })
                .collect(Collectors.toList());
        return getRevenueTimeForDates(dates) / dates.size();
    }
    public long getRevenueTimeForDates (List<LocalDate> dates) {
        Map<String, Long> timePerService = new HashMap<>();

        // First, get revenue time for each service calendar used in feed dates
        // NOTE: we don't simply get revenue time for each individual date of service
        // because that ends up duplicating a lot of operations if service calendars
        // are reused often.
        dates.stream()
                .map(date -> feed.getServicesForDate(date))
                .flatMap(List::stream)
                .collect(Collectors.toSet())
                .stream()
                .forEach(s -> {
                    List<Trip> trips = feed.getTripsForService(s.service_id);
                    long time = this.pattern.getTotalRevenueTimeForTrips(trips);
                    timePerService.put(s.service_id, time);
                });

        // Next, sum up service calendars by dates of service
        long total = dates.stream()
                .map(date ->  // get sum of services per date
                    feed.getServicesForDate(date).stream()
                            .map(s -> timePerService.get(s.service_id))
                            .mapToLong(time -> time)
                            .sum()
                )
                .mapToLong(time -> time)
                .sum();
        return total;
    }

    public long getTotalRevenueTime () {
        return getRevenueTimeForDates(feed.getDatesOfService());
    }

    /** Get total revenue distance (in meters) for all trips on a given date. */
    public double getTotalDistanceForTrips (LocalDate date) {
        return this.pattern.getTotalDistanceForTrips(getTripsForDate(date));
    }

    /** in seconds */
    public int getDailyAverageHeadway (LocalDate date, LocalTime from, LocalTime to) {

        OptionalDouble avg =  feed.stops.values().stream()
                .map(s -> this.stop.getAverageHeadwayForStop(s.stop_id, date, from, to))
                .mapToDouble(headway -> headway)
                .average();

        return (int) avg.getAsDouble();
    }

    public double getAverageTripSpeed (LocalDate date, LocalTime from, LocalTime to) {
        List<Trip> trips = getTripsForDate(date);
        return this.pattern.getAverageSpeedForTrips(trips, from, to);
    }
    public Map<LocalDate, List<Trip>> getTripsPerDateOfService () {
        Map<LocalDate, List<Trip>> tripsPerDate = new HashMap<>();
        Map<String, List<Trip>> tripsPerService = new HashMap<>();

        LocalDate startDate = getStartDate();
        LocalDate endDate = getEndDate();
        if (startDate == null || endDate == null) {
            return tripsPerDate;
        }
        int allDates = (int) ChronoUnit.DAYS.between(startDate, endDate.plus(1, ChronoUnit.DAYS));
        List<LocalDate> dates = IntStream.range(0, allDates)
                .mapToObj(offset -> startDate.plusDays(offset))
                .collect(Collectors.toList());
        dates.stream()
                .map(date -> feed.getServicesForDate(date))
                .flatMap(List::stream)
                .collect(Collectors.toSet())
                .stream()
                .forEach(s -> {
                    List<Trip> trips = feed.getTripsForService(s.service_id);
                    tripsPerService.put(s.service_id, trips);
                });
        dates.stream()
                .forEach(date -> {
                    List<Trip> trips = feed.getServicesForDate(date).stream()
                            .map(s -> tripsPerService.get(s.service_id))
                            .flatMap(List::stream)
                            .collect(Collectors.toList());
                    tripsPerDate.put(date, trips);
                });
        return tripsPerDate;
    }

    public List<Trip> getTripsForDate (LocalDate date) {
        return feed.getServicesForDate(date).stream()
                .map(s -> feed.getTripsForService(s.service_id))
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    public Collection<Agency> getAllAgencies() {
        return feed.agency.values();
    }

    /**
     * Get the bounding box of this GTFS feed.
     * We use a Rectangle2D rather than a Geotools envelope because GTFS is always in WGS 84.
     * Note that stops do not have agencies in GTFS.
     */
    public Rectangle2D getBounds () {
        Rectangle2D ret = null;

        for (Stop stop : feed.stops.values()) {

            // skip over stops that don't have any stop times
            if (!feed.stopCountByStopTime.containsKey(stop.stop_id)) {
                continue;
            }
            if (ret == null) {
                ret = new Rectangle2D.Double(stop.stop_lon, stop.stop_lat, 0, 0);
            }
            else {
                ret.add(new Point2D.Double(stop.stop_lon, stop.stop_lat));
            }
        }

        return ret;
    }

    public AgencyStatistic getStatistic(String agencyId) {
        AgencyStatistic gs = new AgencyStatistic();
        gs.setAgencyId(agencyId);
        gs.setRouteCount(AgencyStats.getRouteCount(feed, agencyId));
        gs.setTripCount(AgencyStats.getTripCount(feed, agencyId));
        gs.setStopCount(AgencyStats.getStopCount(feed, agencyId));
        gs.setStopTimeCount(AgencyStats.getStopTimesCount(feed, agencyId));
        gs.setCalendarStartDate(AgencyStats.getCalendarDateStart(feed, agencyId));
        gs.setCalendarEndDate(AgencyStats.getCalendarDateEnd(feed, agencyId));
        gs.setCalendarServiceStart(AgencyStats.getCalendarServiceRangeStart(feed, agencyId));
        gs.setCalendarServiceEnd(AgencyStats.getCalendarServiceRangeEnd(feed, agencyId));
        gs.setBounds(getBounds());
        return gs;
    }

    public String getStatisticAsCSV(String agencyId) {
        AgencyStatistic s = getStatistic(agencyId);
        return formatStatisticAsCSV(s);
    }

    public static String formatStatisticAsCSV(AgencyStatistic s) {
        StringBuffer buff = new StringBuffer();
        buff.append(s.getAgencyId());
        buff.append(",");
        buff.append(s.getRouteCount());
        buff.append(",");
        buff.append(s.getTripCount());
        buff.append(",");
        buff.append(s.getStopCount());
        buff.append(",");
        buff.append(s.getStopTimeCount());
        buff.append(",");
        buff.append(s.getCalendarServiceStart());
        buff.append(",");
        buff.append(s.getCalendarServiceEnd());
        buff.append(",");
        buff.append(s.getCalendarStartDate());
        buff.append(",");
        buff.append(s.getCalendarEndDate());
        return buff.toString();
    }

    public long getFrequencyCount() {
        return feed.frequencies.size();
    }

    public long getShapePointCount() {
        return feed.shape_points.size();
    }

    public long getFareAttributeCount() {
        return feed.fares.size();
    }

    public long getFareRulesCount() {
        return feed.fares.values().stream()
                .mapToInt(fare -> fare.fare_rules.size())
                .sum();
    }

    public long getServiceCount() {
        return feed.services.size();
    }

    public List<LocalDate> getDatesOfService() {
        return feed.getDatesOfService();
    }

    public Geometry getMergedBuffers() {
        return feed.getMergedBuffers();
    }
}
