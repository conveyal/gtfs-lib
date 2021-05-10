package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

public class Translation extends Entity {

    public String table_name;
    public String field_name;
    public String language;
    public String translation;
    public String record_id;
    public String record_sub_id;
    public String field_value;

    @Override
    public String getId() {
        return createId(table_name, field_name, language);
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#TRANSLATIONS}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, table_name);
        statement.setString(oneBasedIndex++, field_name);
        statement.setString(oneBasedIndex++, language);
        statement.setString(oneBasedIndex++, translation);
        statement.setString(oneBasedIndex++, record_id);
        statement.setString(oneBasedIndex++, record_sub_id);
        statement.setString(oneBasedIndex++, field_value);
    }

    public static class Loader extends Entity.Loader<Translation> {

        public Loader(GTFSFeed feed) {
            super(feed, "translation");
        }

        @Override
        protected boolean isRequired() {
            return false;
        }

        @Override
        public void loadOneRow() throws IOException {
            Translation t = new Translation();
            t.id = row + 1; // offset line number by 1 to account for 0-based row index
            t.table_name = getStringField("table_name", true);
            t.field_name = getStringField("field_name", true);
            t.field_name = getStringField("language", true);
            t.translation = getStringField("translation", true);
            t.record_id = getStringField("record_id", false);
            t.record_sub_id = getStringField("record_sub_id", false);
            t.field_value = getStringField("field_value", false);
            feed.translations.put(
                createId(t.table_name, t.field_name, t.language),
                t
            );
        }
    }

    public static class Writer extends Entity.Writer<Translation> {
        public Writer (GTFSFeed feed) {
            super(feed, "translation");
        }

        @Override
        protected void writeHeaders() throws IOException {
            writer.writeRecord(new String[] {"table_name", "field_name", "language", "translation", "record_id",
                "record_sub_id", "field_value"});
        }

        @Override
        protected void writeOneRow(Translation t) throws IOException {
            writeStringField(t.table_name);
            writeStringField(t.field_name);
            writeStringField(t.language);
            writeStringField(t.translation);
            writeStringField(t.record_id);
            writeStringField(t.record_sub_id);
            writeStringField(t.field_value);
            endRecord();
        }

        @Override
        protected Iterator<Translation> iterator() {
            return feed.translations.values().iterator();
        }
    }

    /**
     * Translation entries have no ID in GTFS so we define one based on the fields in the translation entry.
     */
    private static String createId(String table_name, String field_name, String language) {
        return String.format("%s_%s_%s", table_name, field_name, language);
    }

}
