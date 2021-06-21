package com.conveyal.gtfs.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FeedInfoDTO {
    public int id;
    public String feed_id;
    public String feed_publisher_name;
    public String feed_publisher_url;
    public String feed_lang;
    public String feed_start_date;
    public String feed_end_date;
    public String feed_version;
    public String default_route_color;
    public String default_route_type;
    public String default_lang;
    public String feed_contact_email;
    public String feed_contact_url;
}
