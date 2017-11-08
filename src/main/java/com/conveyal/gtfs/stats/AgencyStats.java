package com.conveyal.gtfs.stats;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.Service;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * Created by landon on 9/2/16.
 */
public class AgencyStats {
    private GTFSFeed feed = null;

    public static Integer getRouteCount(GTFSFeed feed, String agencyId) {
        int count = 0;
        for (Route route : feed.routes.values()) {
            if (agencyId.equals(route.agency_id)) {
                count++;
            }
        }
        return count;
    }

    public static Integer getTripCount(GTFSFeed feed, String agencyId) {
        int count = 0;
        for (Trip trip : feed.trips.values()) {
            Route route = feed.routes.get(trip.route_id);
            if (agencyId.equals(route.agency_id)) {
                count++;
            }
        }
        return count;
    }

    public static Integer getStopCount(GTFSFeed feed, String agencyId) {
        int count = 0;
        for (Stop stop : feed.stops.values()) {
//            AgencyAndId id = stop.stop_id;
//            if (agencyId.equals(id.getAgencyId())) {
            count++;
//            }
        }
        return count;
    }

    public static Integer getStopTimesCount(GTFSFeed feed, String agencyId) {
        int count = 0;
        for (StopTime stopTime : feed.stop_times.values()) {
            Trip trip = feed.trips.get(stopTime.trip_id);
            Route route = feed.routes.get(trip.route_id);
            if (agencyId.equals(route.agency_id)) {
                count++;
            }
        }
        return count;
    }

    public static LocalDate getCalendarServiceRangeStart(GTFSFeed feed, String agencyId) {
        int startDate = 0;
        for (Service service : feed.services.values()) {
//            if (agencyId.equals(service.agency_id)) {
//            if (startDate == 0
//                    || service.calendar.start_date < startDate)
//                startDate = service.calendar.start_date;
//            }
        }
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        return LocalDate.parse(String.valueOf(startDate), formatter);
    }

    public static LocalDate getCalendarServiceRangeEnd(GTFSFeed feed, String agencyId) {
        int endDate = 0;

        for (Service service : feed.services.values()) {
//            if (agencyId.equals(service.agency_id)) {
//            if (endDate == 0
//                    || service.calendar.end_date > endDate)
//                endDate = service.calendar.end_date;
//            }
        }
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        return LocalDate.parse(String.valueOf(endDate), formatter);
    }

    public static LocalDate getCalendarDateStart(GTFSFeed feed, String agencyId) {
        LocalDate startDate = null;
        for (Service service : feed.services.values()) {
            for (LocalDate date : service.calendar_dates.keySet()) {
//                if (agencyId.equals(serviceCalendarDate.getServiceId().getAgencyId())) {
                if (startDate == null
                        || date.isBefore(startDate))
                    startDate = date;
//                }
            }
        }
        return startDate;
    }

    public static LocalDate getCalendarDateEnd(GTFSFeed feed, String agencyId) {
        LocalDate endDate = null;
        for (Service service : feed.services.values()) {
            for (LocalDate date : service.calendar_dates.keySet()) {
//                if (agencyId.equals(serviceCalendarDate.getServiceId().getAgencyId())) {
                if (endDate == null
                        || date.isAfter(endDate))
                    endDate = date;
//                }
            }
        }
        return endDate;
    }
}
