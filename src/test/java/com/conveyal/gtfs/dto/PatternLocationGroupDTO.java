package com.conveyal.gtfs.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * DTO used to model expected {@link com.conveyal.gtfs.model.PatternLocationGroup} JSON structure for the editor. NOTE:
 * reference types (e.g., Integer and Double) are used here in order to model null/empty values in JSON object.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PatternLocationGroupDTO {
    public int id;

    // PatternHalt params
    public String pattern_id;
    public int stop_sequence;

    // PatternLocationGroup params
    public String location_group_id;
    public int pickup_type;
    public int drop_off_type;
    public int timepoint;
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
    public PatternLocationGroupDTO() {}

    public PatternLocationGroupDTO(
        String patternId,
        String location_group_id,
        int stopSequence,
        int flexDefaultTravelTime,
        int flexDefaultZoneTime
    ) {
        this.pattern_id = patternId;
        this.location_group_id = location_group_id;
        this.stop_sequence = stopSequence;
        this.flex_default_travel_time = flexDefaultTravelTime;
        this.flex_default_zone_time = flexDefaultZoneTime;
    }

    public PatternLocationGroupDTO(String pattern_id, String location_group_id, int stop_sequence) {
        this.pattern_id = pattern_id;
        this.location_group_id = location_group_id;
        this.stop_sequence = stop_sequence;
    }
}
