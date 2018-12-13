package com.conveyal.gtfs.stats.model;

import com.conveyal.gtfs.stats.FeedStats;
import org.locationtech.jts.geom.Geometry;

import java.awt.geom.Rectangle2D;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by landon on 10/11/16.
 */
public class FeedStatistic implements Serializable {

    public String feed_id;
    public int headway;
    public Double avgSpeed;
    public long revenueTime;
    public LocalDate startDate;
    public LocalDate endDate;
    public long	agencyCount;
    public long	routeCount;
    public long stopCount;
    public long tripCount;
    public long frequencyCount;
    public long stopTimeCount;
    public long shapePointCount;
    public long fareAttributeCount;
    public long fareRuleCount;
    public long serviceCount;
    public List<LocalDate> datesOfService;
    public Rectangle2D bounds;
    public Geometry mergedBuffers;

    public FeedStatistic (FeedStats stats, LocalDate date, LocalTime from, LocalTime to) {
        feed_id = stats.feed_id;
        headway = stats.getDailyAverageHeadway(date, from, to);
        avgSpeed = stats.getAverageTripSpeed(date, from, to);
        tripCount = stats.getTripCount(date);
        revenueTime = stats.getTotalRevenueTimeForDate(date);
    }

    public FeedStatistic (FeedStats stats) {
        feed_id = stats.feed_id;
        this.startDate = stats.getStartDate();
        this.endDate = stats.getEndDate();
        this.agencyCount = stats.getAgencyCount();
        this.routeCount = stats.getRouteCount();
        this.stopCount = stats.getStopCount();
        this.tripCount = stats.getTripCount();
        this.frequencyCount = stats.getFrequencyCount();
        this.stopTimeCount = stats.getStopTimesCount();
        this.shapePointCount = stats.getShapePointCount();
        this.fareAttributeCount = stats.getFareAttributeCount();
        this.fareRuleCount = stats.getFareRulesCount();
        this.serviceCount= stats.getServiceCount();
        this.datesOfService = stats.getDatesOfService();
        this.bounds = stats.getBounds();
        this.revenueTime = stats.getAverageWeekdayRevenueTime();
        this.mergedBuffers = stats.getMergedBuffers();
    }
}
