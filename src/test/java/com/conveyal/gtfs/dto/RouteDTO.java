package com.conveyal.gtfs.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * DTO used to model expected {@link com.conveyal.gtfs.model.Route} JSON structure for the editor. NOTE: reference types
 * (e.g., Integer and Double) are used here in order to model null/empty values in JSON object.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RouteDTO {
    public int id;
    public String route_id;
    public String agency_id;
    public String route_short_name;
    public String route_long_name;
    public String route_desc;
    public Integer route_type;
    public String route_url;
    public String route_branding_url;
    public String route_color;
    public String route_text_color;
    public Integer publicly_visible;
    public Integer wheelchair_accessible;
    /** This field is incorrectly set to String in order to test how empty string literals are persisted to the database. */
    public String route_sort_order;
    public Integer status;
}
