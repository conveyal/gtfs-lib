package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;

import org.mapdb.Fun;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Represents a GTFS StopTime. Note that once created and saved in a feed, stop times are by convention immutable
 * because they are in a MapDB.
 */
public class StopTime extends Entity implements Cloneable, Serializable {

    private static final long serialVersionUID = -8883780047901081832L;
    /* StopTime cannot directly reference Trips or Stops because they would be serialized into the MapDB. */
    public String trip_id;
    public int    arrival_time = INT_MISSING;
    public int    departure_time = INT_MISSING;
    public String stop_id;
    public int    stop_sequence;
    public String stop_headsign;
    public int    pickup_type;
    public int    drop_off_type;
    public int    continuous_pickup = INT_MISSING;
    public int    continuous_drop_off = INT_MISSING;
    public double shape_dist_traveled = DOUBLE_MISSING;
    public int    timepoint = INT_MISSING;

    // Additional GTFS Flex booking rule fields.
    public String pickup_booking_rule_id;
    public String drop_off_booking_rule_id;

    // Additional GTFS Flex fields.
    public int start_pickup_drop_off_window = INT_MISSING;
    public int end_pickup_drop_off_window = INT_MISSING;
    public double mean_duration_factor = DOUBLE_MISSING;
    public double mean_duration_offset = DOUBLE_MISSING;
    public double safe_duration_factor = DOUBLE_MISSING;
    public double safe_duration_offset = DOUBLE_MISSING;


    @Override
    public String getId() {
        return trip_id; // Needs sequence number to be unique
    }

    @Override
    public Integer getSequenceNumber() {
        return stop_sequence; // Compound key of StopTime is (trip_id, stop_sequence)
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#STOP_TIMES}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, trip_id);
        setIntParameter(statement, oneBasedIndex++, stop_sequence);
        statement.setString(oneBasedIndex++, stop_id);
        setIntParameter(statement, oneBasedIndex++, arrival_time);
        setIntParameter(statement, oneBasedIndex++, departure_time);
        statement.setString(oneBasedIndex++, stop_headsign);
        setIntParameter(statement, oneBasedIndex++, pickup_type);
        setIntParameter(statement, oneBasedIndex++, drop_off_type);
        setIntParameter(statement, oneBasedIndex++, continuous_pickup);
        setIntParameter(statement, oneBasedIndex++, continuous_drop_off);
        statement.setDouble(oneBasedIndex++, shape_dist_traveled);
        setIntParameter(statement, oneBasedIndex++, timepoint);

        // Flex fields
        statement.setString(oneBasedIndex++, pickup_booking_rule_id);
        statement.setString(oneBasedIndex++, drop_off_booking_rule_id);
        setIntParameter(statement, oneBasedIndex++, start_pickup_drop_off_window);
        setIntParameter(statement, oneBasedIndex++, end_pickup_drop_off_window);
        setDoubleParameter(statement, oneBasedIndex++, mean_duration_factor);
        setDoubleParameter(statement, oneBasedIndex++, mean_duration_offset);
        setDoubleParameter(statement, oneBasedIndex++, safe_duration_factor);
        setDoubleParameter(statement, oneBasedIndex, safe_duration_offset);
    }

    public static class Loader extends Entity.Loader<StopTime> {

        private boolean isFlex = feed.isGTFSFlexFeed();

        public Loader(GTFSFeed feed) {
            super(feed, "stop_times");
        }

        @Override
        protected boolean isRequired() {
            return true;
        }

        @Override
        public void loadOneRow() throws IOException {
            StopTime st = new StopTime();
            st.id = row + 1; // offset line number by 1 to account for 0-based row index
            st.trip_id        = getStringField("trip_id", true);
            // TODO: arrival_time and departure time are not required, but if one is present the other should be
            // also, if this is the first or last stop, they are both required
            st.arrival_time   = getTimeField("arrival_time", false);
            st.departure_time = getTimeField("departure_time", false);
            st.stop_id        = getStringField("stop_id", true);
            st.stop_sequence  = getIntField("stop_sequence", true, 0, Integer.MAX_VALUE);
            st.stop_headsign  = getStringField("stop_headsign", false);
            st.pickup_type    = getIntField("pickup_type", false, 0, 3); // TODO add ranges as parameters
            st.drop_off_type  = getIntField("drop_off_type", false, 0, 3);
            st.continuous_pickup = getIntField("continuous_pickup", false, 0, 3, INT_MISSING);
            st.continuous_drop_off = getIntField("continuous_drop_off", false, 0, 3, INT_MISSING);
            st.shape_dist_traveled = getDoubleField("shape_dist_traveled", false, 0D, Double.MAX_VALUE); // FIXME using both 0 and NaN for "missing", define DOUBLE_MISSING
            st.timepoint      = getIntField("timepoint", false, 0, 1, INT_MISSING);
            if (isFlex) {
                st.pickup_booking_rule_id = getStringField("pickup_booking_rule_id", false);
                st.drop_off_booking_rule_id = getStringField("drop_off_booking_rule_id", false);
                st.start_pickup_drop_off_window = getTimeField("start_pickup_drop_off_window", false);
                st.end_pickup_drop_off_window = getTimeField("end_pickup_drop_off_window", false);
                st.mean_duration_factor = getDoubleField("mean_duration_factor", false, 0D, Double.MAX_VALUE);
                st.mean_duration_offset = getDoubleField("mean_duration_offset", false, 0D, Double.MAX_VALUE);
                st.safe_duration_factor = getDoubleField("safe_duration_factor", false, 0D, Double.MAX_VALUE);
                st.safe_duration_offset = getDoubleField("safe_duration_offset", false, 0D, Double.MAX_VALUE);
            }
            st.feed           = null; // this could circular-serialize the whole feed
            feed.stop_times.put(new Fun.Tuple2(st.trip_id, st.stop_sequence), st);

            /*
              Check referential integrity without storing references. StopTime cannot directly reference Trips or
              Stops because they would be serialized into the MapDB.
             */
            getRefField("trip_id", true, feed.trips);
            getRefField("stop_id", true, feed.stops);
        }

    }

    public static class Writer extends Entity.Writer<StopTime> {
        public Writer (GTFSFeed feed) {
            super(feed, "stop_times");
        }

        /**
         * This is the only table which has a mixture of original GTFS values and GTFS Flex values. If the feed does not
         * include GTFS Flex data, the additional headers are not required.
         */
        @Override
        protected void writeHeaders() throws IOException {
            String[] originalHeaders = new String[] {"trip_id", "arrival_time", "departure_time", "stop_id",
                    "stop_sequence", "stop_headsign", "pickup_type", "drop_off_type", "continuous_pickup",
                    "continuous_drop_off", "shape_dist_traveled", "timepoint"};

            String[] flexHeaders = new String[] {"pickup_booking_rule_id", "drop_off_booking_rule_id",
                    "start_pickup_drop_off_window", "end_pickup_drop_off_window", "mean_duration_factor",
                    "mean_duration_offset", "safe_duration_factor", "safe_duration_offset"};

            if (feed.isGTFSFlexFeed()) {
                String[] headers = Arrays.copyOf(originalHeaders, originalHeaders.length + flexHeaders.length);
                System.arraycopy(flexHeaders, 0, headers, originalHeaders.length, flexHeaders.length);
                writer.writeRecord(headers);
            } else {
                writer.writeRecord(originalHeaders);
            }
        }

        @Override
        protected void writeOneRow(StopTime st) throws IOException {
            writeStringField(st.trip_id);
            writeTimeField(st.arrival_time);
            writeTimeField(st.departure_time);
            writeStringField(st.stop_id);
            writeIntField(st.stop_sequence);
            writeStringField(st.stop_headsign);
            writeIntField(st.pickup_type);
            writeIntField(st.drop_off_type);
            writeIntField(st.continuous_pickup);
            writeIntField(st.continuous_drop_off);
            writeDoubleField(st.shape_dist_traveled);
            writeIntField(st.timepoint);
            if (feed.isGTFSFlexFeed()) {
                // Only include these fields if this is a GTFS Flex feed.
                writeStringField(st.pickup_booking_rule_id);
                writeStringField(st.drop_off_booking_rule_id);
                writeTimeField(st.start_pickup_drop_off_window);
                writeTimeField(st.end_pickup_drop_off_window);
                writeDoubleField(st.mean_duration_factor);
                writeDoubleField(st.mean_duration_offset);
                writeDoubleField(st.safe_duration_factor);
                writeDoubleField(st.safe_duration_offset);
            }
            endRecord();
        }

        @Override
        protected Iterator<StopTime> iterator() {
            return feed.stop_times.values().iterator();
        }


    }

    /**
     * Check that the flex column 'start_pickup_drop_off_window' exists in the stop time table. If this column exists
     * it is assumed that the other flex columns do too. This is to guard against cases where booking rules, stop
     * areas or locations are defined in a feed but flex specific stop time columns are not.
     */
    private static boolean flexColumnExist(Connection connection, String tablePrefix) throws SQLException {
        boolean exists = false;
        String sql = String.format(
            "SELECT EXISTS (SELECT 1 " +
            "FROM information_schema.columns " +
            "WHERE table_schema='%s' " +
            "AND table_name='stop_times' " +
            "AND column_name='start_pickup_drop_off_window')",
            tablePrefix.replace(".", "")
        );
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                exists = resultSet.getBoolean(1);
            }
        }
        return exists;
    }

    /**
     * Extract stop times which are required for flex validation. To qualify, one of the flex fields must not be null.
     * To match the expected import values, where applicable, integers and doubles that are null are set to INT_MISSING
     * and DOUBLE_MISSING respectively.
     */
    public static List<StopTime> getFlexStopTimesForValidation(Connection connection, String tablePrefix) throws SQLException {
        List<StopTime> stopTimes = new ArrayList<>();
        if (!flexColumnExist(connection, tablePrefix)) {
            return stopTimes;
        }
        String sql = String.format(
            "select id, trip_id, stop_id, arrival_time, departure_time, pickup_type, drop_off_type, " +
            "start_pickup_drop_off_window, end_pickup_drop_off_window, mean_duration_factor, mean_duration_offset, " +
            "safe_duration_factor, safe_duration_offset " +
            "from %sstop_times where " +
            "start_pickup_drop_off_window IS NOT NULL " +
            "or end_pickup_drop_off_window IS NOT NULL " +
            "or mean_duration_factor IS NOT NULL " +
            "or mean_duration_offset IS NOT NULL " +
            "or safe_duration_factor IS NOT NULL " +
            "or safe_duration_offset IS NOT NULL ",
            tablePrefix
        );
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                StopTime stopTime = new StopTime();
                stopTime.id = resultSet.getInt(1);
                stopTime.trip_id  = resultSet.getString(2);
                stopTime.stop_id  = resultSet.getString(3);
                stopTime.arrival_time  = getIntValue(resultSet.getString(4));
                stopTime.departure_time  = getIntValue(resultSet.getString(5));
                stopTime.pickup_type  = resultSet.getInt(6);
                stopTime.drop_off_type  = resultSet.getInt(7);
                stopTime.start_pickup_drop_off_window  = getIntValue(resultSet.getString(8));
                stopTime.end_pickup_drop_off_window  = getIntValue(resultSet.getString(9));
                stopTime.mean_duration_factor  = getDoubleValue(resultSet.getString(10));
                stopTime.mean_duration_offset  = getDoubleValue(resultSet.getString(11));
                stopTime.safe_duration_factor  = getDoubleValue(resultSet.getString(12));
                stopTime.safe_duration_offset  = getDoubleValue(resultSet.getString(13));
                stopTimes.add(stopTime);
            }
        }
        return stopTimes;
    }

    @Override
    public StopTime clone () {
        try {
            return (StopTime) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StopTime stopTime = (StopTime) o;
        return arrival_time == stopTime.arrival_time &&
                departure_time == stopTime.departure_time &&
                stop_sequence == stopTime.stop_sequence &&
                pickup_type == stopTime.pickup_type &&
                drop_off_type == stopTime.drop_off_type &&
                continuous_pickup == stopTime.continuous_pickup &&
                continuous_drop_off == stopTime.continuous_drop_off &&
                Double.compare(stopTime.shape_dist_traveled, shape_dist_traveled) == 0 &&
                timepoint == stopTime.timepoint &&
                start_pickup_drop_off_window == stopTime.start_pickup_drop_off_window &&
                end_pickup_drop_off_window == stopTime.end_pickup_drop_off_window &&
                Double.compare(stopTime.mean_duration_factor, mean_duration_factor) == 0 &&
                Double.compare(stopTime.mean_duration_offset, mean_duration_offset) == 0 &&
                Double.compare(stopTime.safe_duration_factor, safe_duration_factor) == 0 &&
                Double.compare(stopTime.safe_duration_offset, safe_duration_offset) == 0 &&
                Objects.equals(trip_id, stopTime.trip_id) &&
                Objects.equals(stop_id, stopTime.stop_id) &&
                Objects.equals(stop_headsign, stopTime.stop_headsign) &&
                Objects.equals(pickup_booking_rule_id, stopTime.pickup_booking_rule_id) &&
                Objects.equals(drop_off_booking_rule_id, stopTime.drop_off_booking_rule_id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                trip_id,
                arrival_time,
                departure_time,
                stop_id,
                stop_sequence,
                stop_headsign,
                pickup_type,
                drop_off_type,
                continuous_pickup,
                continuous_drop_off,
                shape_dist_traveled,
                timepoint,
                pickup_booking_rule_id,
                drop_off_booking_rule_id,
                start_pickup_drop_off_window,
                end_pickup_drop_off_window,
                mean_duration_factor,
                mean_duration_offset,
                safe_duration_factor,
                safe_duration_offset
        );
    }
}
