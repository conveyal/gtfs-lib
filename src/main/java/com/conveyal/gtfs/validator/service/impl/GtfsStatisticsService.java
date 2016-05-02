package com.conveyal.gtfs.validator.service.impl;

import com.conveyal.gtfs.model.Statistic;
import com.conveyal.gtfs.service.StatisticsService;
import org.onebusaway.gtfs.model.Agency;
import org.onebusaway.gtfs.model.AgencyAndId;
import org.onebusaway.gtfs.model.Route;
import org.onebusaway.gtfs.model.ServiceCalendar;
import org.onebusaway.gtfs.model.ServiceCalendarDate;
import org.onebusaway.gtfs.model.Stop;
import org.onebusaway.gtfs.model.StopTime;
import org.onebusaway.gtfs.model.Trip;
import org.onebusaway.gtfs.services.GtfsDao;

import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.util.Collection;
import java.util.Date;

/**
 * Retrieves a base set of statistics from the GTFS.
 * 
 */
public class GtfsStatisticsService implements StatisticsService {

  private GtfsDao gtfsDao = null;

  public GtfsStatisticsService(GtfsDao dao) {
    gtfsDao = dao;
  }

  public Integer getAgencyCount() {
    return gtfsDao.getAllAgencies().size();
  }

  public Integer getRouteCount() {
    return gtfsDao.getAllRoutes().size();
  }

  public Integer getTripCount() {
    return gtfsDao.getAllTrips().size();
  }

  public Integer getStopCount() {
    return gtfsDao.getAllStops().size();
  }

  public Integer getStopTimesCount() {
    return gtfsDao.getAllStopTimes().size();
  }

  // calendar date range start/end assume a service calendar based schedule
  // returns null for schedules without calendar service schedules

  public Date getCalendarServiceRangeStart() {

    Date startDate = null;

    for (ServiceCalendar serviceCalendar : gtfsDao.getAllCalendars()) {

      if (startDate == null
          || serviceCalendar.getStartDate().getAsDate().before(startDate))
        startDate = serviceCalendar.getStartDate().getAsDate();
    }

    return startDate;

  }

  public Date getCalendarServiceRangeEnd() {

    Date endDate = null;

    for (ServiceCalendar serviceCalendar : gtfsDao.getAllCalendars()) {

      if (endDate == null
          || serviceCalendar.getEndDate().getAsDate().after(endDate))
        endDate = serviceCalendar.getEndDate().getAsDate();
    }

    return endDate;
  }

  public Date getCalendarDateStart() {

    Date startDate = null;

    for (ServiceCalendarDate serviceCalendarDate : gtfsDao.getAllCalendarDates()) {

      if (startDate == null
          || serviceCalendarDate.getDate().getAsDate().before(startDate))
        startDate = serviceCalendarDate.getDate().getAsDate();
    }

    return startDate;

  }

  public Date getCalendarDateEnd() {

    Date endDate = null;

    for (ServiceCalendarDate serviceCalendarDate : gtfsDao.getAllCalendarDates()) {

      if (endDate == null
          || serviceCalendarDate.getDate().getAsDate().after(endDate))
        endDate = serviceCalendarDate.getDate().getAsDate();
    }

    return endDate;
  }

  public Collection<Agency> getAllAgencies() {
    return gtfsDao.getAllAgencies();
  }

  public Integer getRouteCount(String agencyId) {
    int count = 0;
    Collection<Route> routes = gtfsDao.getAllRoutes();
    for (Route route : routes) {
      if (agencyId.equals(route.getAgency().getId())) {
        count++;
      }
    }
    return count;
  }

  public Integer getTripCount(String agencyId) {
    int count = 0;
    Collection<Trip> trips = gtfsDao.getAllTrips();
    for (Trip trip : trips) {
      if (agencyId.equals(trip.getRoute().getAgency().getId())) {
        count++;
      }
    }
    return count;
  }

  public Integer getStopCount(String agencyId) {
    int count = 0;
    Collection<Stop> stops = gtfsDao.getAllStops();
    for (Stop stop : stops) {
      AgencyAndId id = stop.getId();
      if (agencyId.equals(id.getAgencyId())) {
        count++;
      }
    }
    return count;
  }

  public Integer getStopTimesCount(String agencyId) {
    int count = 0;
    Collection<StopTime> stopTimes = gtfsDao.getAllStopTimes();
    for (StopTime stopTime : stopTimes) {
      if (agencyId.equals(stopTime.getTrip().getRoute().getAgency().getId())) {
        count++;
      }
    }
    return count;
  }

  public Date getCalendarServiceRangeStart(String agencyId) {

    Date startDate = null;

    for (ServiceCalendar serviceCalendar : gtfsDao.getAllCalendars()) {
      if (agencyId.equals(serviceCalendar.getServiceId().getAgencyId())) {
        if (startDate == null
            || serviceCalendar.getStartDate().getAsDate().before(startDate))
          startDate = serviceCalendar.getStartDate().getAsDate();
      }
    }

    return startDate;

  }

  public Date getCalendarServiceRangeEnd(String agencyId) {
    Date endDate = null;
    for (ServiceCalendar serviceCalendar : gtfsDao.getAllCalendars()) {
      if (agencyId.equals(serviceCalendar.getServiceId().getAgencyId())) {
        if (endDate == null
            || serviceCalendar.getEndDate().getAsDate().after(endDate))
          endDate = serviceCalendar.getEndDate().getAsDate();
      }
    }

    return endDate;
  }

  public Date getCalendarDateStart(String agencyId) {

    Date startDate = null;

    for (ServiceCalendarDate serviceCalendarDate : gtfsDao.getAllCalendarDates()) {
      if (agencyId.equals(serviceCalendarDate.getServiceId().getAgencyId())) {
        if (startDate == null
            || serviceCalendarDate.getDate().getAsDate().before(startDate))
          startDate = serviceCalendarDate.getDate().getAsDate();
      }
    }

    return startDate;

  }

  public Date getCalendarDateEnd(String agencyId) {
    Date endDate = null;
    for (ServiceCalendarDate serviceCalendarDate : gtfsDao.getAllCalendarDates()) {
      if (agencyId.equals(serviceCalendarDate.getServiceId().getAgencyId())) {
        if (endDate == null
            || serviceCalendarDate.getDate().getAsDate().after(endDate))
          endDate = serviceCalendarDate.getDate().getAsDate();
      }
    }

    return endDate;
  }
  
  /**
   * Get the bounding box of this GTFS feed.
   * We use a Rectangle2D rather than a Geotools envelope because GTFS is always in WGS 84.
   * Note that stops do not have agencies in GTFS.
   */
  public Rectangle2D getBounds () {
      Rectangle2D ret = null;
      
      for (Stop stop : gtfsDao.getAllStops()) {
          if (ret == null) {
              ret = new Rectangle2D.Double(stop.getLon(), stop.getLat(), 0, 0);
          }
          else {
              ret.add(new Point2D.Double(stop.getLon(), stop.getLat()));
          }
      }
  
      return ret;
  }

  public Statistic getStatistic(String agencyId) {
    Statistic gs = new Statistic();
    gs.setAgencyId(agencyId);
    gs.setRouteCount(getRouteCount(agencyId));
    gs.setTripCount(getTripCount(agencyId));
    gs.setStopCount(getStopCount(agencyId));
    gs.setStopTimeCount(getStopTimesCount(agencyId));
    gs.setCalendarStartDate(getCalendarDateStart(agencyId));
    gs.setCalendarEndDate(getCalendarDateEnd(agencyId));
    gs.setCalendarServiceStart(getCalendarServiceRangeStart(agencyId));
    gs.setCalendarServiceEnd(getCalendarServiceRangeEnd(agencyId));
    gs.setBounds(getBounds());
    return gs;
  }

  public String getStatisticAsCSV(String agencyId) {
    Statistic s = getStatistic(agencyId);
    return formatStatisticAsCSV(s);
    
  }

  public static String formatStatisticAsCSV(Statistic s) {
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
