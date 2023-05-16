package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

public class Area extends Entity {

    private static final long serialVersionUID = -7958476364523575940L;
    public String area_id;
    public String area_name;

    @Override
    public String getId () {
        return area_id;
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#AREA}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, area_id);
        statement.setString(oneBasedIndex, area_name);
    }

    public static class Loader extends Entity.Loader<Area> {

        public Loader(GTFSFeed feed) {
            super(feed, "areas");
        }

        @Override
        protected boolean isRequired() {
            return true;
        }

        @Override
        public void loadOneRow() throws IOException {
            Area a = new Area();
            a.id = row + 1; // offset line number by 1 to account for 0-based row index
            a.area_id = getStringField("area_id", false);
            a.area_name  = getStringField("area_name", false);
            // Attempting to put a null key or value will cause an NPE in BTreeMap
            if (a.area_id != null) {
                feed.areas.put(a.area_id, a);
            }
        }

    }

    public static class Writer extends Entity.Writer<Area> {
        public Writer(GTFSFeed feed) {
            super(feed, "areas");
        }

        @Override
        public void writeHeaders() throws IOException {
            writer.writeRecord(new String[] {"area_id", "area_name"});
        }

        @Override
        public void writeOneRow(Area a) throws IOException {
            writeStringField(a.area_id);
            writeStringField(a.area_name);
            endRecord();
        }

        @Override
        public Iterator<Area> iterator() {
            return this.feed.areas.values().iterator();
        }
    }
}
