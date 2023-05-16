package com.conveyal.gtfs.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * DTO used to model expected {@link com.conveyal.gtfs.model.PatternStopArea} JSON structure for the editor. NOTE:
 * reference types (e.g., Integer and Double) are used here in order to model null/empty values in JSON object.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PatternStopAreaDTO {
    public int id;

    // PatternHalt params
    public String pattern_id;
    public int stop_sequence;

    // PatternStopArea params
    public String area_id;
    public int pickup_type;
    public int drop_off_type;
    public int timepoint;
    public String stop_headsign;
    public int continuous_pickup;
    public int continuous_drop_off;

    public String pickup_booking_rule_id;
    public String drop_off_booking_rule_id;

    public int flex_default_travel_time;
    public int flex_default_zone_time;
    public double mean_duration_factor;
    public double mean_duration_offset;
    public double safe_duration_factor;
    public double safe_duration_offset;

    /** Empty constructor for deserialization */
    public PatternStopAreaDTO() {}

    public PatternStopAreaDTO(
        String patternId,
        String area_id,
        int stopSequence,
        int flexDefaultTravelTime,
        int flexDefaultZoneTime
    ) {
        this.pattern_id = patternId;
        this.area_id = area_id;
        this.stop_sequence = stopSequence;
        this.flex_default_travel_time = flexDefaultTravelTime;
        this.flex_default_zone_time = flexDefaultZoneTime;
    }

    public PatternStopAreaDTO(String pattern_id, String area_id, int stop_sequence) {
        this.pattern_id = pattern_id;
        this.area_id = area_id;
        this.stop_sequence = stop_sequence;
    }
}
