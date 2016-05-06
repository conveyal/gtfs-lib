package com.conveyal.gtfs.validator.model;

import java.awt.geom.Rectangle2D;
import java.util.Date;

/**
 * Model object representing statistics about GTFS. 
 *
 */
public class Statistic {
	private String agencyId;
	private Integer routeCount;
	private Integer tripCount;
	private Integer stopCount;
	private Integer stopTimeCount;
	private Date calendarServiceStart;
	private Date calendarServiceEnd;
	private Date calendarStartDate;
	private Date calendarEndDate;
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
	public Date getCalendarStartDate() {
		return calendarStartDate;
	}
	public void setCalendarStartDate(Date calendarStartDate) {
		this.calendarStartDate = calendarStartDate;
	}
	public Date getCalendarEndDate() {
		return calendarEndDate;
	}
	public void setCalendarEndDate(Date calendarEndDate) {
		this.calendarEndDate = calendarEndDate;
	}
	public Date getCalendarServiceStart() {
		return calendarServiceStart;
	}
	public void setCalendarServiceStart(Date calendarServiceStart) {
		this.calendarServiceStart = calendarServiceStart;
	}
	public Date getCalendarServiceEnd() {
		return calendarServiceEnd;
	}
	public void setCalendarServiceEnd(Date calendarServiceEnd) {
		this.calendarServiceEnd = calendarServiceEnd;
	}
        public Rectangle2D getBounds() {
            return bounds;
        }
        public void setBounds(Rectangle2D bounds) {
            this.bounds = bounds;
        }	
}
