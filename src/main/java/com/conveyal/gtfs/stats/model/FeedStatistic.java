package com.conveyal.gtfs.stats.model;

import com.conveyal.gtfs.stats.FeedStats;

import java.time.LocalDate;
import java.time.LocalTime;

/**
 * Created by landon on 10/11/16.
 */
public class FeedStatistic {

    public String pattern_id;
    public int headway;
    public Double avgSpeed;
    public long tripCount;
    public double revenueTime;
//    public Double avgSpeedOffPeak;
//    private LocalDate calendarServiceEnd;
//    private LocalDate calendarStartDate;
//    private LocalDate calendarEndDate;
//    private Rectangle2D bounds;

    public FeedStatistic (FeedStats stats, String pattern_id, LocalDate date, LocalTime from, LocalTime to) {
        this.pattern_id = pattern_id;
        headway = stats.getDailyAverageHeadway(date, from, to);
        avgSpeed = stats.getAverageTripSpeed(date, from, to);
        tripCount = stats.getTripCount(date);
        revenueTime = stats.getTotalRevenueTimeForTrips(date);
    }
}
