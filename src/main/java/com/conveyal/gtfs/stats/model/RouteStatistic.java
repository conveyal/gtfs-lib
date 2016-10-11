package com.conveyal.gtfs.stats.model;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.stats.FeedStats;
import com.conveyal.gtfs.stats.RouteStats;

import java.awt.geom.Rectangle2D;
import java.time.LocalDate;
import java.time.LocalTime;

/**
 * Created by landon on 9/2/16.
 */
public class RouteStatistic {
    public String route_id;
    public String routeName;
    public int headway;
    public Double avgSpeed;
    public long tripCount;
    public double stopSpacing;
//    public Double avgSpeedOffPeak;
//    private LocalDate calendarServiceEnd;
//    private LocalDate calendarStartDate;
//    private LocalDate calendarEndDate;
//    private Rectangle2D bounds;

    public RouteStatistic (RouteStats stats, String route_id, LocalDate date, LocalTime from, LocalTime to) {
        this.route_id = route_id;
        routeName = stats.getRouteName(route_id);
        // TODO: add fields for inbound and outbound directions
        headway = stats.getHeadwayForRouteDirection(this.route_id, 0, date, from, to);
        avgSpeed = stats.getSpeedForRouteDirection(this.route_id, 0, date, from, to);
        tripCount = stats.getTripCountForDate(this.route_id, date);
        stopSpacing = stats.getAverageStopSpacing(this.route_id);
    }

    public static String getHeaderAsCsv () {
        StringBuffer buffer = new StringBuffer();

        buffer.append("route_id");
        buffer.append(",");
        buffer.append("routeName");
        buffer.append(",");
        buffer.append("headway");
        buffer.append(",");
        buffer.append("avgSpeed");

        return buffer.toString();
    }
    public String asCsv () {
        StringBuffer buffer = new StringBuffer();

        buffer.append(route_id);
        buffer.append(",");
        buffer.append(routeName);
        buffer.append(",");
        buffer.append(headway);
        buffer.append(",");
        buffer.append(avgSpeed);

        return buffer.toString();
    }
}
