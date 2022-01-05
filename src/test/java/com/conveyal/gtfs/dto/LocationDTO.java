package com.conveyal.gtfs.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.net.URL;

/**
 * DTO used to model expected {@link com.conveyal.gtfs.model.Location} JSON structure for the editor. NOTE:
 * reference types (e.g., Integer and Double) are used here in order to model null/empty values in JSON object.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class LocationDTO {
    public int id;
    public String location_id;
    public String stop_name;
    public String stop_desc;
    public String zone_id;
    public URL stop_url;
    public String geometry_type;
}
