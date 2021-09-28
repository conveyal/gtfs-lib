package com.conveyal.gtfs.loader;

import org.junit.jupiter.api.Test;

import static com.conveyal.gtfs.loader.JdbcGtfsLoader.POSTGRES_NULL_TEXT;
import static com.conveyal.gtfs.loader.Requirement.OPTIONAL;
import static com.conveyal.gtfs.loader.Requirement.REQUIRED;
import static com.conveyal.gtfs.loader.Table.FARE_ATTRIBUTES;
import static com.conveyal.gtfs.loader.Table.ROUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;

class LineContextTest {
    /**
     * Emulates querying a field in a GTFS record for which the column is absent in the source GTFS.
     * Here we query the optional "contains_id" field that is missing in the "fare_rules" table.
     */
    @Test
    void shouldReturnPostgresNullTextForMissingField() {
        // Line number, fare_id, route_id
        // (Optional field "contains_id" is missing.)
        String[] rowDataWithLineNumber = new String[] {"2", "1", "300"};
        Table table = Table.FARE_RULES;

        // Here, only list fields that are loaded from the GTFS feed, as happens during execution.
        Field[] fields = new Field[] {
            new StringField("fare_id", REQUIRED).isReferenceTo(FARE_ATTRIBUTES),
            new StringField("route_id", OPTIONAL).isReferenceTo(ROUTES),
        };
        LineContext lineContext = new LineContext(table, fields, rowDataWithLineNumber, 2);

        assertEquals(POSTGRES_NULL_TEXT, lineContext.getValueForRow("contains_id"));
    }
}
