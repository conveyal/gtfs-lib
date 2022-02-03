package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Objects;

public class LocationGroup extends Entity {

    private static final long serialVersionUID = -4961539668114167098L;

    public String location_group_id;
    public String location_id;
    public String location_group_name;

    @Override
    public String getId() {
        return location_group_id;
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#LOCATION_GROUPS}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, location_group_id);
        statement.setString(oneBasedIndex++, location_id);
        statement.setString(oneBasedIndex++, location_group_name);
    }

    public static class Loader extends Entity.Loader<LocationGroup> {

        public Loader(GTFSFeed feed) {
            super(feed, "location_groups");
        }

        @Override
        protected boolean isRequired() {
            return false;
        }

        @Override
        public void loadOneRow() throws IOException {
            LocationGroup locationGroup = new LocationGroup();
            locationGroup.id = row + 1; // offset line number by 1 to account for 0-based row index
            locationGroup.location_group_id = getStringField("location_group_id", true);
            locationGroup.location_id = getStringField("location_id", false);
            locationGroup.location_group_name = getStringField("location_group_name", false);
            // Attempting to put a null key or value will cause an NPE in BTreeMap
            if (locationGroup.location_group_id != null) {
                feed.locationGroups.put(locationGroup.location_group_id, locationGroup);
            }
        }
    }

    public static class Writer extends Entity.Writer<LocationGroup> {
        public Writer(GTFSFeed feed) {
            super(feed, "location_groups");
        }

        @Override
        public void writeHeaders() throws IOException {
            writer.writeRecord(new String[] {"location_group_id", "location_id", "location_group_name"});
        }

        @Override
        public void writeOneRow(LocationGroup locationGroup) throws IOException {
            writeStringField(locationGroup.location_group_id);
            writeStringField(locationGroup.location_id);
            writeStringField(locationGroup.location_group_name);
            endRecord();
        }

        @Override
        public Iterator<LocationGroup> iterator() {
            return this.feed.locationGroups.values().iterator();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LocationGroup that = (LocationGroup) o;
        return Objects.equals(location_group_id, that.location_group_id) &&
            Objects.equals(location_id, that.location_id) &&
            Objects.equals(location_group_name, that.location_group_name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(location_group_id, location_id, location_group_name);
    }
}
