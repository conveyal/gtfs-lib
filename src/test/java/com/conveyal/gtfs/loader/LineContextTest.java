package com.conveyal.gtfs.loader;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNull;

class LineContextTest {
    /**
     * Emulates querying a field in a GTFS record for which the column is absent in the source GTFS.
     * Here we query the optional "contains_id" field that is missing in the "fare_rules" table.
     */
    @Test
    void shouldReturnNullForMissingField() {
        // Line number, fare_id, route_id
        // (Optional field "contains_id" is missing.)
        String[] rowDataWithLineNumber = new String[] {"2", "1", "300"};
        Table table = Table.FARE_RULES;
        LineContext lineContext = new LineContext(table, table.fields, rowDataWithLineNumber, 2);

        assertNull(lineContext.getValueForRow("contains_id"));
    }
}
