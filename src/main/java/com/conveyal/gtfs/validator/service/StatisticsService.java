package com.conveyal.gtfs.validator.service;

import com.conveyal.gtfs.validator.model.Statistic;

import java.awt.geom.Rectangle2D;
import java.time.LocalDate;

/**
 *	Provides statistics for:
 * <or> 
 * <li>Agencies
 * <li>Routes
 * <li>Trips
 * <li>Stops
 * <li>Stop Times
 * <li>Calendar Date ranges
 * <li>Calendar Service exceptions
 * </or>
 * @author dev
 *
 */
public interface StatisticsService {

	Integer getAgencyCount();

	Integer getRouteCount();

	Integer getTripCount();

	Integer getStopCount();

	Integer getStopTimesCount();
	
	LocalDate getCalendarDateStart();
	
	LocalDate getCalendarDateEnd();

	LocalDate getCalendarServiceRangeStart();

	LocalDate getCalendarServiceRangeEnd();

	Integer getRouteCount(String agencyId);

	Integer getTripCount(String agencyId);

	Integer getStopCount(String agencyId);

	Integer getStopTimesCount(String agencyId);

	LocalDate getCalendarDateStart(String agencyId);
		
	LocalDate getCalendarDateEnd(String agencyId);
	
	LocalDate getCalendarServiceRangeStart(String agencyId);

	LocalDate getCalendarServiceRangeEnd(String agencyId);
	
	Rectangle2D getBounds();

	Statistic getStatistic(String agencyId);
}
