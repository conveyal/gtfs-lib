package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Objects;

public class LocationShape extends Entity {

    private static final long serialVersionUID = -972419107947161195L;

    public String shape_id;
    public double shape_pt_lat = DOUBLE_MISSING;
    public double shape_pt_lon = DOUBLE_MISSING;
    public int shape_pt_sequence = INT_MISSING;
    public String location_meta_data_id;

    @Override
    public String getId() {
        return shape_id;
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#LOCATION_SHAPES}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, shape_id);
        setDoubleParameter(statement, oneBasedIndex++, shape_pt_lat);
        setDoubleParameter(statement, oneBasedIndex++, shape_pt_lon);
        setIntParameter(statement, oneBasedIndex++, shape_pt_sequence);
        statement.setString(oneBasedIndex++, location_meta_data_id);
    }

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
            locationShape.shape_id = getStringField("shape_id", true);
            locationShape.shape_pt_lat = getDoubleField("shape_pt_lat", true, 0D, Double.MAX_VALUE);
            locationShape.shape_pt_lon = getDoubleField("shape_pt_lon", true, 0D, Double.MAX_VALUE);
            locationShape.shape_pt_sequence = getIntField("shape_pt_sequence", true, 0, Integer.MAX_VALUE);
            locationShape.location_meta_data_id = getStringField("location_meta_data_id", true);

            // Attempting to put a null key or value will cause an NPE in BTreeMap
            if (locationShape.shape_id != null) {
                feed.locationShapes.put(locationShape.shape_id, locationShape);
            }
        }
    }

    public static String header() {
        return "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence,location_meta_data_id\n";
    }

    public String toCsvRow() {
        return shape_id + "," + shape_pt_lat + "," + shape_pt_lon + "," + shape_pt_sequence + "," + location_meta_data_id + "\n";
    }


    public static class Writer extends Entity.Writer<LocationShape> {
        public Writer(GTFSFeed feed) {
            super(feed, "location_shapes");
        }

        @Override
        public void writeHeaders() throws IOException {
            writer.writeRecord(new String[]{"shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence", "location_meta_data_id"});
        }

        @Override
        public void writeOneRow(LocationShape locationShape) throws IOException {
            writeStringField(locationShape.shape_id);
            writeDoubleField(locationShape.shape_pt_lat);
            writeDoubleField(locationShape.shape_pt_lon);
            writeIntField(locationShape.shape_pt_sequence);
            writeStringField(locationShape.location_meta_data_id);
            endRecord();
        }

        @Override
        public Iterator<LocationShape> iterator() {
            return this.feed.locationShapes.values().iterator();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LocationShape shape = (LocationShape) o;
        return Double.compare(shape.shape_pt_lat, shape_pt_lat) == 0 && Double.compare(shape.shape_pt_lon, shape_pt_lon) == 0 && shape_pt_sequence == shape.shape_pt_sequence && Objects.equals(shape_id, shape.shape_id) && Objects.equals(location_meta_data_id, shape.location_meta_data_id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, location_meta_data_id);
    }

    @Override
    public String toString() {
        return "LocationShape{" +
            "shape_id='" + shape_id + '\'' +
            ", shape_pt_lat=" + shape_pt_lat +
            ", shape_pt_lon=" + shape_pt_lon +
            ", shape_pt_sequence=" + shape_pt_sequence +
            ", location_meta_data_id='" + location_meta_data_id + '\'' +
            '}';
    }
}