package com.conveyal.gtfs.util;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * DTO used to model expected {@link com.conveyal.gtfs.model.FareRule} JSON structure.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FareRuleDTO {
    public int id;
    public String fare_id;
    public String route_id;
    public String contains_id;
    public String origin_id;
    public String destination_id;
}
