package com.conveyal.gtfs.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.net.URL;

/**
 * DTO used to model expected {@link com.conveyal.gtfs.model.BookingRule} JSON structure for the editor. NOTE: reference types
 * (e.g., Integer and Double) are used here in order to model null/empty values in JSON object.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class BookingRuleDTO {
    public int id;
    public String booking_rule_id;
    public int booking_type;
    public int prior_notice_duration_min;
    public int prior_notice_duration_max;
    public int prior_notice_last_day;
    public String prior_notice_last_time;
    public int prior_notice_start_day;
    public String prior_notice_start_time;
    public String prior_notice_service_id;
    public String message;
    public String pickup_message;
    public String drop_off_message;
    public String phone_number;
    public URL info_url;
    public URL booking_url;
}
