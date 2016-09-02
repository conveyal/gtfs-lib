package com.conveyal.gtfs.stats;

import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.model.Agency;
import com.conveyal.gtfs.model.Service;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.Trip;
import com.conveyal.gtfs.stats.model.AgencyStatistic;

/**
 * Retrieves a base set of statistics from the GTFS.
 *
 */
public class FeedStats {

    private GTFSFeed feed = null;

    public FeedStats(GTFSFeed f) {
        feed = f;
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
        LocalDate feedStartDate = !feed.feedInfo.isEmpty()
                ? feed.feedInfo.values().iterator().next().feed_start_date
                : null;
        LocalDate startDate = feedStartDate != null
                ? feedStartDate
                : getCalendarServiceRangeStart() != null
                ? getCalendarDateStart()
                : null;
        return startDate;
    }

    public LocalDate getEndDate() {
        LocalDate feedEndDate = !feed.feedInfo.isEmpty()
                ? feed.feedInfo.values().iterator().next().feed_end_date
                : null;
        LocalDate endDate = feedEndDate != null
                ? feedEndDate
                : getCalendarServiceRangeEnd() != null
                ? getCalendarDateEnd()
                : null;
        return endDate;
    }
    public Map<LocalDate, List<Trip>> getTripsPerDateOfService() {

        Map<String, List<Trip>> tripsPerService = getTripsPerService();
        Map<LocalDate, List<Trip>> tripsPerDate = new HashMap<>();

        LocalDate startDate = getStartDate();
        LocalDate endDate = getEndDate();


        // loop through services
        for (Service service : feed.services.values()) {

            // skip service if start or end date is null
            if (startDate == null || endDate == null) {
                continue;
            }

            // iterate through each date between start and end date
            for (LocalDate date = startDate; date.isBefore(endDate); date = date.plusDays(1)) {
                List<Trip> tripList = getTripsForDate(date);
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

    public Map<String, List<Trip>> getTripsPerService () {
        Map<String, List<Trip>> tripsPerService = new HashMap<>();

        feed.trips.values().stream().forEach(trip -> {
            List<Trip> tripsList = tripsPerService.get(trip.service_id);
            if (tripsList == null) {
                tripsList = new ArrayList<>();
            }
            tripsList.add(trip);
            tripsPerService.put(trip.service_id, tripsList);
        });
        return tripsPerService;
    }

    public List<Trip> getTripsForDate (LocalDate date) {
        List<Trip> trips = new ArrayList<>();

        // loop through services
        for (Service service : feed.services.values()) {
            // if service is active on given day, add all trips that operate under that service
            if (service.activeOn(date)) {
                List<Trip> serviceTrips = feed.trips.values().stream().filter(trip -> trip.service_id.equals(service.service_id)).collect(Collectors.toList());
                if (serviceTrips != null)
                    trips.addAll(serviceTrips);

            }
        }
        return trips;
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
}
