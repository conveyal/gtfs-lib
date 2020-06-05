package com.conveyal.gtfs.dto;

public class StopTimeDTO {
    public String trip_id;
    public String stop_id;
    public Integer stop_sequence;
    public Integer arrival_time;
    public Integer departure_time;
    public String stop_headsign;
    public Integer timepoint;
    public Integer drop_off_type;
    public Integer pickup_type;
    public Double shape_dist_traveled;

    /**
     * Empty constructor for deserialization
     */
    public StopTimeDTO () {}

    public StopTimeDTO (String stopId, Integer arrivalTime, Integer departureTime, Integer stopSequence) {
        stop_id = stopId;
        arrival_time = arrivalTime;
        departure_time = departureTime;
        stop_sequence = stopSequence;
    }
}
