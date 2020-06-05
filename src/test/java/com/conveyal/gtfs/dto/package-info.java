/**
 * This package contains Data Transfer Objects that are solely used for efficient serialization and deserialization of
 * data into and out of JSON strings in unit tests, which the {@link com.conveyal.gtfs.loader.JdbcTableWriter#update(java.lang.Integer, java.lang.String, boolean)}
 * and {@link com.conveyal.gtfs.loader.JdbcTableWriter#create(java.lang.String, boolean)} methods take as input in order
 * to modify or create GTFS entities. These DTOs are not used internally by the JdbcTableWriter in favor of simply
 * deserializing JSON into {@link com.fasterxml.jackson.databind.JsonNode} objects and validating the fields using
 * {@link com.conveyal.gtfs.loader.Table} definitions. This approach was chosen because it allows the update and create
 * methods to generically process the JSON and update the data based on the rules and relations defined by the Tables.
 *
 * The value/wisdom of maintaining these objects for tests alone has been posed and is up for debate, but they do offer
 * an advantage over generating JSON with Jackson because JdbcTableWriter expects input JSON to have a value defined for
 * each field for a given entity (even if it is just null). Using Jackson for this purpose would become verbose. They
 * also might be better suited for tests than {@link com.conveyal.gtfs.model} objects because they do not use primitive
 * types, which allows for testing bad inputs (though as of 2019/02/13 we are not testing for bad inputs AFAIK).
 */

package com.conveyal.gtfs.dto;

