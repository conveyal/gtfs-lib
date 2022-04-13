package com.conveyal.gtfs.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * DTO used to model expected {@link com.conveyal.gtfs.model.PatternLocation} JSON structure for the editor. NOTE:
 * reference types (e.g., Integer and Double) are used here in order to model null/empty values in JSON object.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PatternLocationDTO {
    public int id;

    // PatternHalt params
    public String pattern_id;
    public int stop_sequence;

    // PatternLocation params
    public String location_id;
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
    public PatternLocationDTO() {}

    public PatternLocationDTO (String patternId, String locationId, int stopSequence) {
        pattern_id = patternId;
        location_id = locationId;
        stop_sequence = stopSequence;
    }

    public PatternLocationDTO (
        String patternId,
        String locationId,
        int stopSequence,
        int flexDefaultTravelTime,
        int flexDefaultZoneTime
    ) {
        pattern_id = patternId;
        location_id = locationId;
        stop_sequence = stopSequence;
        flex_default_travel_time = flexDefaultTravelTime;
        flex_default_zone_time = flexDefaultZoneTime;
    }

}
