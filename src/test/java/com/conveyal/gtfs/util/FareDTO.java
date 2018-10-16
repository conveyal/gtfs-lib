package com.conveyal.gtfs.util;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * DTO used to model expected fare JSON structure. NOTE: reference types (e.g., Integer and Double) are used here in
 * order to model null/empty values in JSON object.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FareDTO {
    public int id;
    public String fare_id;
    public Double price;
    public String currency_type;
    public Integer payment_method;
    public Integer transfers;
    public String agency_id;
    public Double transfer_duration;
    public FareRuleDTO[] fare_rules;
}
