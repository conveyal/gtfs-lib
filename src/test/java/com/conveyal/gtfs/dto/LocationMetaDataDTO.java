package com.conveyal.gtfs.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * DTO used to model expected {@link com.conveyal.gtfs.model.LocationMetaData} JSON structure for the editor. NOTE:
 * reference types (e.g., Integer and Double) are used here in order to model null/empty values in JSON object.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class LocationMetaDataDTO {
    public int id;
    public String location_meta_data_id;
    public String properties;
    public String geometry_type;
}
