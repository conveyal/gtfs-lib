package com.conveyal.gtfs.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * DTO used to model expected {@link com.conveyal.gtfs.model.Fare} JSON structure. NOTE: reference types (e.g., Integer
 * and Double) are used here in order to model null/empty values in JSON object.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FareDTO {
    public int id;
    public String fare_id;
    public Double price;
    public String currency_type;
    public Integer payment_method;
    // transfers is a string because we need to be able to pass empty strings to the JdbcTableWriter
    public String transfers;
    public String agency_id;
    public Integer transfer_duration;
    public FareRuleDTO[] fare_rules;
}
