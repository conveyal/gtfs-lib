package com.conveyal.gtfs.loader;

import static com.conveyal.gtfs.loader.JdbcGtfsLoader.POSTGRES_NULL_TEXT;

/**
 * Wrapper class that provides access to row values and line context (e.g., line number) for a particular row of GTFS
 * data.
 */
public class LineContext {
    public final Table table;
    private final Field[] fields;
    /**
     * The row data has one extra value at the beginning of the array that represents the line number.
     */
    private final String[] rowDataWithLineNumber;
    public final int lineNumber;

    public LineContext(Table table, Field[] fields, String[] rowDataWithLineNumber, int lineNumber) {
        this.table = table;
        this.fields = fields;
        this.rowDataWithLineNumber = rowDataWithLineNumber;
        this.lineNumber = lineNumber;
    }

    /**
     * Get value for a particular column index from a set of row data. Note: the row data here has one extra value at
     * the beginning of the array that represents the line number (hence the +1). This is because the data is formatted
     * for batch insertion into a postgres table.
     */
    public String getValueForRow(int columnIndex) {
        return rowDataWithLineNumber[columnIndex + 1];
    }

    /**
     * Overloaded method to provide value for the current line for a particular field.
     * @param fieldName The name of the GTFS field/column to retrieve
     * @return The value for the field, if found, or POSTGRES_NULL_TEXT if the field is not found,
     *         consistent with cases where the value is null in the postgres database.
     */
    public String getValueForRow(String fieldName) {
        int fieldIndex = Field.getFieldIndex(fields, fieldName);
        return fieldIndex >= 0
            ? rowDataWithLineNumber[fieldIndex + 1]
            : POSTGRES_NULL_TEXT;
    }

    /**
     * Overloaded method to provide value for the current line for the key field.
     */
    public String getEntityId() {
        return getValueForRow(table.getKeyFieldIndex(fields));
    }
}
