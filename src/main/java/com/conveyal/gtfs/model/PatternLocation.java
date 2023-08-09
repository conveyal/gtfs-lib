package com.conveyal.gtfs.model;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * A pattern location represents generalized information about a location visited by a pattern, i.e. a collection of trips that
 * all visit the same locations in the same sequence. Some of these characteristics, e.g., location ID, stop sequence, pickup
 * type, and drop off type, help determine a unique pattern. Others (default dwell/travel time, timepoint, and shape dist
 * traveled) are specific to the editor and usually based on values from the first trip encountered in a feed for a
 * given pattern.
 */
public class PatternLocation extends PatternHalt {
    private static final long serialVersionUID = 1L;

    public String location_id;

    public int pickup_type;
    public int drop_off_type;
    public int timepoint;
    public String stop_headsign;
    public int continuous_pickup = INT_MISSING;
    public int continuous_drop_off = INT_MISSING;

    // Additional GTFS Flex fields.
    public String pickup_booking_rule_id;
    public String drop_off_booking_rule_id;
    public int flex_default_travel_time = INT_MISSING;
    public int flex_default_zone_time = INT_MISSING;
    public double mean_duration_factor = DOUBLE_MISSING;
    public double mean_duration_offset = DOUBLE_MISSING;
    public double safe_duration_factor = DOUBLE_MISSING;
    public double safe_duration_offset = DOUBLE_MISSING;

    public PatternLocation () {}

    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, pattern_id);
        // Stop sequence is zero-based.
        setIntParameter(statement, oneBasedIndex++, stop_sequence);
        statement.setString(oneBasedIndex++, location_id);
        setIntParameter(statement, oneBasedIndex++, drop_off_type);
        setIntParameter(statement, oneBasedIndex++, pickup_type);
        setIntParameter(statement, oneBasedIndex++, timepoint);
        statement.setString(oneBasedIndex++, stop_headsign);
        setIntParameter(statement, oneBasedIndex++, continuous_pickup);
        setIntParameter(statement, oneBasedIndex++, continuous_drop_off);
        statement.setString(oneBasedIndex++, pickup_booking_rule_id);
        statement.setString(oneBasedIndex++, drop_off_booking_rule_id);

        // the derived fields
        setIntParameter(statement, oneBasedIndex++, flex_default_travel_time);
        setIntParameter(statement, oneBasedIndex++, flex_default_zone_time);

        // the copied fields
        setDoubleParameter(statement, oneBasedIndex++, mean_duration_factor);
        setDoubleParameter(statement, oneBasedIndex++, mean_duration_offset);
        setDoubleParameter(statement, oneBasedIndex++, safe_duration_factor);
        setDoubleParameter(statement, oneBasedIndex, safe_duration_offset);
    }

    public int getTravelTime() {
        return flex_default_travel_time == Entity.INT_MISSING ? 0 : flex_default_travel_time;
    }

    public int getDwellTime() {
        return flex_default_zone_time == Entity.INT_MISSING ? 0 : flex_default_zone_time;
    }

    public boolean isFlex() {
        return true;
    }
}
