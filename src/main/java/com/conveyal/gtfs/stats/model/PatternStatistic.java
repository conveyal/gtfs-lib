package com.conveyal.gtfs.stats.model;

import com.conveyal.gtfs.stats.PatternStats;

import java.time.LocalDate;
import java.time.LocalTime;

/**
 * Created by landon on 10/11/16.
 */
public class PatternStatistic {

    public String pattern_id;
    public int headway;
    public Double avgSpeed;
//    public Double avgSpeedOffPeak;
//    private LocalDate calendarServiceEnd;
//    private LocalDate calendarStartDate;
//    private LocalDate calendarEndDate;
//    private Rectangle2D bounds;

    public PatternStatistic (PatternStats stats, String pattern_id, LocalDate date, LocalTime from, LocalTime to) {
        this.pattern_id = pattern_id;
        headway = stats.getHeadwayForPattern(this.pattern_id, date, from, to);
        avgSpeed = stats.getPatternSpeed(this.pattern_id, date, from, to);
    }
}
