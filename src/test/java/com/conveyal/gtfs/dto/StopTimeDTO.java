package com.conveyal.gtfs.dto;

public class StopTimeDTO {
    public int id;
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
    public int continuous_pickup;
    public int continuous_drop_off;

    // Additional GTFS Flex booking rule fields.
    public String pickup_booking_rule_id;
    public String drop_off_booking_rule_id;

    // Additional GTFS Flex location groups and locations fields
    public Integer start_pickup_dropoff_window;
    public Integer end_pickup_dropoff_window;
    public double mean_duration_factor;
    public double mean_duration_offset;
    public double safe_duration_factor;
    public double safe_duration_offset;

    /**
     * Empty constructor for deserialization
     */
    public StopTimeDTO() {
    }

    public StopTimeDTO(String stopId, Integer arrivalTime, Integer departureTime, Integer stopSequence) {
        stop_id = stopId;
        arrival_time = arrivalTime;
        departure_time = departureTime;
        stop_sequence = stopSequence;
    }
}
