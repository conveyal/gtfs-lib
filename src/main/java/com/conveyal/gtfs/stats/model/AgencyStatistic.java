package com.conveyal.gtfs.stats.model;

import java.awt.geom.Rectangle2D;
import java.time.LocalDate;

/**
 * Model object representing statistics about GTFS. 
 *
 */
public class AgencyStatistic {
	private String agencyId;
	private Integer routeCount;
	private Integer tripCount;
	private Integer stopCount;
	private Integer stopTimeCount;
	private LocalDate calendarServiceStart;
	private LocalDate calendarServiceEnd;
	private LocalDate calendarStartDate;
	private LocalDate calendarEndDate;
	private Rectangle2D bounds;
	
	public String getAgencyId() {
		return agencyId;
	}
	public void setAgencyId(String agencyId) {
		this.agencyId = agencyId;
	}
	public Integer getRouteCount() {
		return routeCount;
	}
	public void setRouteCount(Integer routeCount) {
		this.routeCount = routeCount;
	}
	public Integer getTripCount() {
		return tripCount;
	}
	public void setTripCount(Integer tripCount) {
		this.tripCount = tripCount;
	}
	public Integer getStopCount() {
		return stopCount;
	}
	public void setStopCount(Integer stopCount) {
		this.stopCount = stopCount;
	}
	public Integer getStopTimeCount() {
		return stopTimeCount;
	}
	public void setStopTimeCount(Integer stopTimeCount) {
		this.stopTimeCount = stopTimeCount;
	}
	public LocalDate getCalendarStartDate() {
		return calendarStartDate;
	}
	public void setCalendarStartDate(LocalDate calendarStartDate) {
		this.calendarStartDate = calendarStartDate;
	}
	public LocalDate getCalendarEndDate() {
		return calendarEndDate;
	}
	public void setCalendarEndDate(LocalDate calendarEndDate) {
		this.calendarEndDate = calendarEndDate;
	}
	public LocalDate getCalendarServiceStart() {
		return calendarServiceStart;
	}
	public void setCalendarServiceStart(LocalDate calendarServiceStart) {
		this.calendarServiceStart = calendarServiceStart;
	}
	public LocalDate getCalendarServiceEnd() {
		return calendarServiceEnd;
	}
	public void setCalendarServiceEnd(LocalDate calendarServiceEnd) {
		this.calendarServiceEnd = calendarServiceEnd;
	}
        public Rectangle2D getBounds() {
            return bounds;
        }
        public void setBounds(Rectangle2D bounds) {
            this.bounds = bounds;
        }	
}
