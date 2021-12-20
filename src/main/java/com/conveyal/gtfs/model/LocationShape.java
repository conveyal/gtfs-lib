package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.util.Util;
import org.locationtech.jts.geom.Coordinate;
import org.mapdb.Fun;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class LocationShape extends Entity{

    private static final long serialVersionUID = -972419107947161195L;

    public String location_id;
    public String geometry_id;
    public String geometry_type;
    public double geometry_pt_lat;
    public double geometry_pt_lon;

    public LocationShape () {}

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
        statement.setString(oneBasedIndex++, geometry_type);
        statement.setDouble(oneBasedIndex++, geometry_pt_lat);
        statement.setDouble(oneBasedIndex++, geometry_pt_lon);
    }

}

//    public int shape_polygon_id = INT_MISSING;
//    public int shape_ring_id = INT_MISSING;
//    public int shape_line_string_id = INT_MISSING;
//    public double shape_pt_lat = DOUBLE_MISSING;
//    public double shape_pt_lon = DOUBLE_MISSING;
//    public int shape_pt_sequence = INT_MISSING;
//    public String location_meta_data_id;

//    @Override
//    public String getId() {
//        return location_id;
//    }

//    public int getShape_polygon_id() {
//        return shape_polygon_id;
//    }

//    public int getShape_ring_id() {
//        return shape_ring_id;
//    }

//    public int getShape_pt_sequence() {
//        return shape_pt_sequence;
//    }

//    public int getShape_line_string_id() {
//        return shape_line_string_id;
//    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#LOCATION_SHAPES}. JDBC prepared statement parameters use a one-based index.
     */
//    @Override
//    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
//        int oneBasedIndex = 1;
//        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
//        statement.setString(oneBasedIndex++, location_id);
//        statement.setString(oneBasedIndex++, polygon_id);
//        statement.setString(oneBasedIndex++, geometry_type);
//        statement.setString()
////        statement.setString(oneBasedIndex++, location_id);
////        setIntParameter(statement, oneBasedIndex++, shape_polygon_id);
////        setIntParameter(statement, oneBasedIndex++, shape_ring_id);
////        setIntParameter(statement, oneBasedIndex++, shape_line_string_id);
////        setDoubleParameter(statement, oneBasedIndex++, shape_pt_lat);
////        setDoubleParameter(statement, oneBasedIndex++, shape_pt_lon);
////        setIntParameter(statement, oneBasedIndex++, shape_pt_sequence);
////        statement.setString(oneBasedIndex, location_meta_data_id);
//    }

//    public static class Loader extends Entity.Loader<LocationShape> {
//
//        public Loader(GTFSFeed feed) {
//            super(feed, "location_shapes");
//        }
//
//        @Override
//        protected boolean isRequired() {
//            return false;
//        }
//
//        @Override
//        public void loadOneRow() throws IOException {
//            LocationShape locationShape = new LocationShape();
//            locationShape.id = row + 1; // offset line number by 1 to account for 0-based row index
//            locationShape.shape_id = getStringField("shape_id", true);
//            locationShape.shape_polygon_id = getIntField("shape_polygon_id", true, 0, Integer.MAX_VALUE);
//            locationShape.shape_ring_id = getIntField("shape_ring_id", true, 0, Integer.MAX_VALUE);
//            locationShape.shape_line_string_id = getIntField("shape_line_string_id", true, 0, Integer.MAX_VALUE);
//            locationShape.shape_pt_lat = getDoubleField("shape_pt_lat", true, 0D, Double.MAX_VALUE);
//            locationShape.shape_pt_lon = getDoubleField("shape_pt_lon", true, 0D, Double.MAX_VALUE);
//            locationShape.shape_pt_sequence = getIntField("shape_pt_sequence", true, 0, Integer.MAX_VALUE);
//            locationShape.location_meta_data_id = getStringField("location_meta_data_id", true);
//
//            // Attempting to put a null key or value will cause an NPE in BTreeMap
//            if (locationShape.shape_id != null) {
//                feed.locationShapes.put(locationShape.shape_id, locationShape);
//            }
//        }
//    }

    /**
     * Required by {@link com.conveyal.gtfs.util.GeoJsonUtil#getCsvReaderFromGeoJson(String, ZipFile, ZipEntry)} as part
     * of the unpacking of GeoJson data to CSV.
     */
//    public static String header() {
//        return "shape_id,shape_polygon_id,shape_ring_id,shape_line_string_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence,location_meta_data_id\n";
//    }

    /**
     * Required by {@link com.conveyal.gtfs.util.GeoJsonUtil#getCsvReaderFromGeoJson(String, ZipFile, ZipEntry)} as part
     * of the unpacking of GeoJson data to CSV.
     */
//    public String toCsvRow() {
//        return shape_id + "," +
//            shape_polygon_id + "," +
//            shape_ring_id + "," +
//            shape_line_string_id + "," +
//            shape_pt_lat + "," +
//            shape_pt_lon + "," +
//            shape_pt_sequence + "," +
//            location_meta_data_id + "\n";
//    }

//    @Override
//    public boolean equals(Object o) {
//        if (this == o) return true;
//        if (o == null || getClass() != o.getClass()) return false;
//        LocationShape that = (LocationShape) o;
//        return shape_polygon_id == that.shape_polygon_id &&
//            shape_ring_id == that.shape_ring_id &&
//            shape_line_string_id == that.shape_line_string_id &&
//            Double.compare(that.shape_pt_lat, shape_pt_lat) == 0 &&
//            Double.compare(that.shape_pt_lon, shape_pt_lon) == 0 &&
//            shape_pt_sequence == that.shape_pt_sequence &&
//            Objects.equals(shape_id, that.shape_id) &&
//            Objects.equals(location_meta_data_id, that.location_meta_data_id);
//    }

//    @Override
//    public int hashCode() {
//        return Objects.hash(shape_id, shape_polygon_id, shape_ring_id, shape_line_string_id, shape_pt_lat, shape_pt_lon,
//            shape_pt_sequence, location_meta_data_id);
//    }

//    @Override
//    public String toString() {
//        return "LocationShape{" +
//            "location_id='" + location_id + '\'' +
//            ", polygon_id=" + polygon_id +
//            ", geometry_type=" + geometry_type +
//            '}';
//    }
//}