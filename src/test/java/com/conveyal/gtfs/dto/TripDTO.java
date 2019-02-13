package com.conveyal.gtfs.dto;

public class TripDTO {
    public Integer id;
    public String trip_id;
    public String trip_headsign;
    public String trip_short_name;
    public String block_id;
    public Integer direction_id;
    public String route_id;
    public String service_id;
    public Integer wheelchair_accessible;
    public Integer bikes_allowed;
    public String shape_id;
    public String pattern_id;
    public StopTimeDTO[] stop_times;
    public FrequencyDTO[] frequencies;
}
