package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Objects;

public class ShapePoint extends Entity {

    private static final long serialVersionUID = 6751814959971086070L;
    public String shape_id;
    public double shape_pt_lat;
    public double shape_pt_lon;
    public int    shape_pt_sequence;
    public double shape_dist_traveled;

    @Override
    public String getId () {
        return shape_id;
    }

    @Override
    public Integer getSequenceNumber() {
        return shape_pt_sequence;
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#SHAPES}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, shape_id);
        setIntParameter(statement, oneBasedIndex++, shape_pt_sequence);
        statement.setDouble(oneBasedIndex++, shape_pt_lat);
        statement.setDouble(oneBasedIndex++, shape_pt_lon);
        statement.setDouble(oneBasedIndex++, shape_dist_traveled);
        // Editor-specific field below (point_type 0 indicates no control point)
        statement.setInt(oneBasedIndex++, 0);
    }

    public ShapePoint () { }

    // Similar to stoptime, we have to have a constructor, because fields are final so as to be immutable for storage in MapDB.
    public ShapePoint(String shape_id, double shape_pt_lat, double shape_pt_lon, int shape_pt_sequence, double shape_dist_traveled) {
        this.shape_id = shape_id;
        this.shape_pt_lat = shape_pt_lat;
        this.shape_pt_lon = shape_pt_lon;
        this.shape_pt_sequence = shape_pt_sequence;
        this.shape_dist_traveled = shape_dist_traveled;
    }

    public static class Loader extends Entity.Loader<ShapePoint> {

        public Loader(GTFSFeed feed) {
            super(feed, "shapes");
        }

        @Override
        protected boolean isRequired() {
            return false;
        }

        @Override
        public void loadOneRow() throws IOException {
            String shape_id = getStringField("shape_id", true);
            double shape_pt_lat = getDoubleField("shape_pt_lat", true, -90D, 90D);
            double shape_pt_lon = getDoubleField("shape_pt_lon", true, -180D, 180D);
            int shape_pt_sequence = getIntField("shape_pt_sequence", true, 0, Integer.MAX_VALUE);
            double shape_dist_traveled = getDoubleField("shape_dist_traveled", false, 0D, Double.MAX_VALUE);

            ShapePoint s = new ShapePoint(shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled);
            s.id = row + 1; // offset line number by 1 to account for 0-based row index
            s.feed = null; // since we're putting this into MapDB, we don't want circular serialization
            feed.shape_points.put(new Object[]{s.shape_id, s.shape_pt_sequence}, s);
        }
    }

    public static class Writer extends Entity.Writer<ShapePoint> {
        public Writer (GTFSFeed feed) {
            super(feed, "shapes");
        }

        @Override
        protected void writeHeaders() throws IOException {
            writer.writeRecord(new String[] {"shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence", "shape_dist_traveled"});
        }

        @Override
        protected void writeOneRow(ShapePoint s) throws IOException {
            writeStringField(s.shape_id);
            writeDoubleField(s.shape_pt_lat);
            writeDoubleField(s.shape_pt_lon);
            writeIntField(s.shape_pt_sequence);
            writeDoubleField(s.shape_dist_traveled);
            endRecord();
        }

        @Override
        protected Iterator<ShapePoint> iterator() {
            return feed.shape_points.values().iterator();
        }
    }

    public static class MapDBSerializer implements Serializer<ShapePoint> {

        @Override
        public void serialize (
            @NotNull DataOutput2 output,
            @NotNull ShapePoint shapePoint
        ) throws IOException {
            output.writeUTF(shapePoint.shape_id);
            output.writeDouble(shapePoint.shape_pt_lat);
            output.writeDouble(shapePoint.shape_pt_lon);
            output.writeInt(shapePoint.shape_pt_sequence);
            output.writeDouble(shapePoint.shape_dist_traveled);
        }

        @Override
        public ShapePoint deserialize (
            @NotNull DataInput2 input,
            int available
        ) throws IOException {
            ShapePoint shapePoint = new ShapePoint();
            shapePoint.shape_id = input.readUTF();
            shapePoint.shape_pt_lat = input.readDouble();
            shapePoint.shape_pt_lon = input.readDouble();
            shapePoint.shape_pt_sequence = input.readInt();
            shapePoint.shape_dist_traveled = input.readDouble();
            return shapePoint;
        }

        @Override
        public boolean isTrusted () {
            return true;
        }

        @Override
        public boolean equals (Object obj) {
            return Objects.equals(this, obj);
        }
    }
}
