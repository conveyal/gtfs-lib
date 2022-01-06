package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;

import java.io.IOException;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class Location extends Entity {

    private static final long serialVersionUID = -3961639608144161095L;

    public String location_id;
    public String stop_name;
    public String stop_desc;
    public String zone_id;
    public URL stop_url;
    public String geometry_type;

    @Override
    public String getId() {
        return location_id;
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#LOCATIONS}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, location_id);
        statement.setString(oneBasedIndex++, stop_name);
        statement.setString(oneBasedIndex++, stop_desc);
        statement.setString(oneBasedIndex++, zone_id);
        statement.setString(oneBasedIndex++, stop_url != null ? stop_url.toString() : null);
        statement.setString(oneBasedIndex++, geometry_type);
    }

    /**
     * Required by {@link com.conveyal.gtfs.util.GeoJsonUtil#getCsvReaderFromGeoJson(String, ZipFile, ZipEntry)} as part
     * of the unpacking of GeoJson data to CSV.
     */
    public static String header() {
        return "location_id,stop_name,stop_desc,zone_id,stop_url,geometry_type\n";
    }

    /**
     * Required by {@link com.conveyal.gtfs.util.GeoJsonUtil#getCsvReaderFromGeoJson(String, ZipFile, ZipEntry)} as part
     * of the unpacking of GeoJson data to CSV.
     */
    public String toCsvRow() {
        return location_id + "," + stop_name + "," + stop_desc + "," + zone_id + "," + stop_url + "," + geometry_type + "\n";
    }

    public static class Loader extends Entity.Loader<Location> {

        public Loader(GTFSFeed feed) {
            super(feed, "locations");
        }

        @Override
        protected boolean isRequired() {
            return false;
        }

        @Override
        public void loadOneRow() throws IOException {
            Location location = new Location();

            location.id = row + 1;
            location.stop_name = getStringField("stop_name", false);
            location.stop_desc = getStringField("stop_desc", false);
            location.zone_id = getStringField("zone_id", false);
            location.stop_url = getUrlField("stop_url", false);
            // Must be a geometry associated w/ a location
            location.geometry_type = getStringField("geometry_type", true);

            // Attempting to put a null key or value will cause an NPE in BTreeMap
            if (location.location_id != null) {
                feed.locations.put(location.location_id, location);
            }
        }
    }

    public static class Writer extends Entity.Writer<Location> {
        public Writer(GTFSFeed feed) {
            super(feed, "locations");
        }

        @Override
        public void writeHeaders() throws IOException {
            writer.writeRecord(new String[]{"location_id", "stop_name", "zone_id", "stop_url", "geometry_type"});
        }

        @Override
        public void writeOneRow(Location locations) throws IOException {
            writeStringField(locations.location_id);
            writeStringField(locations.zone_id);
            writeStringField(locations.stop_name);
            writeStringField(locations.stop_desc);
            writeUrlField(locations.stop_url);
            writeStringField(locations.geometry_type);
            endRecord();
        }

        @Override
        public Iterator<Location> iterator() {
            return this.feed.locations.values().iterator();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Location that = (Location) o;
        return stop_name == that.stop_name &&
                zone_id == that.zone_id &&
                stop_desc == that.stop_desc &&
                Objects.equals(stop_url, that.stop_url) &&
                Objects.equals(location_id, that.location_id) &&
                geometry_type == that.geometry_type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                location_id,
                stop_name,
                stop_desc,
                stop_url,
                zone_id,
                geometry_type
        );
    }
}
