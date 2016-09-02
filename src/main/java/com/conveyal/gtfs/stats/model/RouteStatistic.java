package com.conveyal.gtfs.stats.model;

import com.conveyal.gtfs.GTFSFeed;

import java.awt.geom.Rectangle2D;
import java.time.LocalDate;

/**
 * Created by landon on 9/2/16.
 */
public class RouteStatistic {
//    private GTFSFeed feed;
    public String routeId;
    public String routeName;
    public Double headwayPeak;
    public Double headwayOffPeak;
    public Double avgSpeedPeak;
    public Double avgSpeedOffPeak;
//    private LocalDate calendarServiceEnd;
//    private LocalDate calendarStartDate;
//    private LocalDate calendarEndDate;
//    private Rectangle2D bounds;

    public RouteStatistic () {
    }

    public static String getHeaderAsCsv () {
        StringBuffer buffer = new StringBuffer();

        buffer.append("routeId");
        buffer.append(",");
        buffer.append("routeName");
        buffer.append(",");
        buffer.append("headwayPeak");
        buffer.append(",");
        buffer.append("headwayOffPeak");
        buffer.append(",");
        buffer.append("avgSpeedPeak");
        buffer.append(",");
        buffer.append("avgSpeedOffPeak");

        return buffer.toString();
    }
    public String asCsv () {
        StringBuffer buffer = new StringBuffer();

        buffer.append(routeId);
        buffer.append(",");
        buffer.append(routeName);
        buffer.append(",");
        buffer.append(headwayPeak);
        buffer.append(",");
        buffer.append(headwayOffPeak);
        buffer.append(",");
        buffer.append(avgSpeedPeak);
        buffer.append(",");
        buffer.append(avgSpeedOffPeak);

        return buffer.toString();
    }
}
