package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class LocationShape extends Entity {

    private static final long serialVersionUID = -972419107947161195L;

    public String location_id;
    public String geometry_id;
    public double geometry_pt_lat;
    public double geometry_pt_lon;

    public LocationShape() {
    }

    @Override
    public String getId() {
        return location_id;
    }

    public String getGeometry_id() {
        return geometry_id;
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#PATTERN_STOP}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, location_id);
        statement.setString(oneBasedIndex++, geometry_id);
        statement.setDouble(oneBasedIndex++, geometry_pt_lat);
        statement.setDouble(oneBasedIndex++, geometry_pt_lon);
    }

    /**
     * This load method is required by {@link GTFSFeed#loadFromFile(ZipFile, String)}
     */
    public static class Loader extends Entity.Loader<LocationShape> {

        public Loader(GTFSFeed feed) {
            super(feed, "location_shapes");
        }

        @Override
        protected boolean isRequired() {
            return false;
        }

        @Override
        public void loadOneRow() throws IOException {
            LocationShape locationShape = new LocationShape();
            locationShape.id = row + 1; // offset line number by 1 to account for 0-based row index
            locationShape.location_id = getStringField("location_id", true);
            locationShape.geometry_id = getStringField("geometry_id", true);
            locationShape.geometry_pt_lat = getDoubleField("geometry_pt_lat", true, -90D, 90D); // reuse lat/lon min and max from Stop class
            locationShape.geometry_pt_lon = getDoubleField("geometry_pt_lon", true, -180D, 180D);

            // Location id can not be used here because it is not unique.
            feed.locationShapes.put(Integer.toString(row), locationShape);
        }
    }

    /**
     * Required by {@link com.conveyal.gtfs.util.GeoJsonUtil#getCsvReaderFromGeoJson(String, ZipFile, ZipEntry, List)}
     * as part of the unpacking of GeoJson data to CSV.
     */
    public static String header() {
        return "location_id,geometry_id,geometry_pt_lat,geometry_pt_lon\n";
    }

    /**
     * Required by {@link com.conveyal.gtfs.util.GeoJsonUtil#getCsvReaderFromGeoJson(String, ZipFile, ZipEntry, List)}
     * as part of the unpacking of GeoJson data to CSV.
     */
    public String toCsvRow() {
        return String.join(
            ",",
            location_id,
            geometry_id,
            Double.toString(geometry_pt_lat),
            Double.toString(geometry_pt_lon)
        ) + System.lineSeparator();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LocationShape that = (LocationShape) o;
        return Double.compare(that.geometry_pt_lat, geometry_pt_lat) == 0 &&
            Double.compare(that.geometry_pt_lon, geometry_pt_lon) == 0 &&
            Objects.equals(location_id, that.location_id) &&
            Objects.equals(geometry_id, that.geometry_id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(location_id, geometry_id, geometry_pt_lat, geometry_pt_lon);
    }
}


