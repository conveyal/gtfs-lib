package com.conveyal.gtfs.stats.model;

import com.conveyal.gtfs.stats.StopStats;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalTime;

/**
 * Created by landon on 10/4/16.
 */
public class StopStatistic implements Serializable {
    public String stop_id;
    public int headway;
    public int routeCount;
    public int tripCount;
//    public TransferPerformanceSummary transferPerformanceSummary;

    public StopStatistic (StopStats stats, String stop_id, LocalDate date, LocalTime from, LocalTime to) {
        this.stop_id = stop_id;
        headway = stats.getAverageHeadwayForStop(stop_id, date, from, to);
        routeCount = stats.getRouteCount(stop_id);
        tripCount = stats.getTripCountForDate(stop_id, date); // TODO: filter by time window?
//        transferPerformanceSummary = new TransferPerformanceSummary(stats, stop_id, date);
    }
}
