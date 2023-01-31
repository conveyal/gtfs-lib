package com.conveyal.gtfs.model;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * A pattern stop represents generalized information about a stop visited by a pattern, i.e. a collection of trips that
 * all visit the same stops in the same sequence. Some of these characteristics, e.g., stop ID, stop sequence, pickup
 * type, and drop off type, help determine a unique pattern. Others (default dwell/travel time, timepoint, and shape dist
 * traveled) are specific to the editor and usually based on values from the first trip encountered in a feed for a
 * given pattern.
 */
public class PatternStop extends PatternHalt {
    private static final long serialVersionUID = 1L;

    public String stop_id;
    // FIXME: Should we be storing default travel and dwell times here?
    public int default_travel_time;
    public int default_dwell_time;
    public double shape_dist_traveled;
    public int pickup_type;
    public int drop_off_type;
    public int timepoint;
    public String stop_headsign;
    public int continuous_pickup = INT_MISSING;
    public int continuous_drop_off = INT_MISSING;

    // Flex additions.
    public String pickup_booking_rule_id;
    public String drop_off_booking_rule_id;

    public PatternStop () {}

    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, pattern_id);
        // Stop sequence is zero-based.
        setIntParameter(statement, oneBasedIndex++, stop_sequence);
        statement.setString(oneBasedIndex++, stop_id);
        statement.setString(oneBasedIndex++, stop_headsign);
        setIntParameter(statement, oneBasedIndex++, default_travel_time);
        setIntParameter(statement, oneBasedIndex++, default_dwell_time);
        setIntParameter(statement, oneBasedIndex++, drop_off_type);
        setIntParameter(statement, oneBasedIndex++, pickup_type);
        setDoubleParameter(statement, oneBasedIndex++, shape_dist_traveled);
        setIntParameter(statement, oneBasedIndex++, timepoint);
        setIntParameter(statement, oneBasedIndex++, continuous_pickup);
        setIntParameter(statement, oneBasedIndex++, continuous_drop_off);
        statement.setString(oneBasedIndex++, pickup_booking_rule_id);
        statement.setString(oneBasedIndex, drop_off_booking_rule_id);
    }
}
