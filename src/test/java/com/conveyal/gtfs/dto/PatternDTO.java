package com.conveyal.gtfs.dto;

public class PatternDTO {
    public Integer id;
    public String pattern_id;
    public String shape_id;
    public String route_id;
    public Integer direction_id;
    public Integer use_frequency;
    public String name;
    public PatternStopDTO[] pattern_stops;
    public PatternLocationDTO[] pattern_locations;
    public PatternStopAreaDTO[] pattern_stop_areas;
    public ShapePointDTO[] shapes;
}