package com.conveyal.gtfs.validator.service;

import com.conveyal.gtfs.model.Statistic;

import java.awt.geom.Rectangle2D;
import java.util.Date;

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
	
	Date getCalendarDateStart();
	
	Date getCalendarDateEnd();

	Date getCalendarServiceRangeStart();

	Date getCalendarServiceRangeEnd();

	Integer getRouteCount(String agencyId);

	Integer getTripCount(String agencyId);

	Integer getStopCount(String agencyId);

	Integer getStopTimesCount(String agencyId);

	Date getCalendarDateStart(String agencyId);
		
	Date getCalendarDateEnd(String agencyId);
	
	Date getCalendarServiceRangeStart(String agencyId);

	Date getCalendarServiceRangeEnd(String agencyId);
	
	Rectangle2D getBounds();

	Statistic getStatistic(String agencyId);
}
