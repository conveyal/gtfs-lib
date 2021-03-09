package com.conveyal.gtfs.dto;

public class PatternStopDTO {
    public Integer id;
    public String pattern_id;
    public String stop_id;
    public Integer default_travel_time;
    public Integer default_dwell_time;
    public Double shape_dist_traveled;
    public Integer drop_off_type;
    public Integer pickup_type;
    public Integer stop_sequence;
    public Integer timepoint;

    /** Empty constructor for deserialization */
    public PatternStopDTO() {}

    public PatternStopDTO (String patternId, String stopId, int stopSequence) {
        pattern_id = patternId;
        stop_id = stopId;
        stop_sequence = stopSequence;
    }
}