package com.conveyal.gtfs.model;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * A pattern location group represents generalized information about a location or stop visited by a pattern, i.e. a
 * collection of trips that all visit the same locations/stops in the same sequence. Some of these characteristics,
 * e.g. location group ID and stop sequence help determine a unique pattern.
 */
public class PatternLocationGroup extends PatternHalt {
    private static final long serialVersionUID = 1L;

    public String location_group_id;

    public int pickup_type;
    public int drop_off_type;
    public int timepoint;
    public int continuous_pickup = INT_MISSING;
    public int continuous_drop_off = INT_MISSING;

    // Flex additions.
    public String pickup_booking_rule_id;
    public String drop_off_booking_rule_id;

    // Additional GTFS Flex location groups and locations fields
    public int flex_default_travel_time = INT_MISSING;
    public int flex_default_zone_time = INT_MISSING;
    public double mean_duration_factor = DOUBLE_MISSING;
    public double mean_duration_offset = DOUBLE_MISSING;
    public double safe_duration_factor = DOUBLE_MISSING;
    public double safe_duration_offset = DOUBLE_MISSING;

    public PatternLocationGroup () {}

     @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
         int oneBasedIndex = 1;
         if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
         statement.setString(oneBasedIndex++, pattern_id);
         // Stop sequence is zero-based.
         setIntParameter(statement, oneBasedIndex++, stop_sequence);
         statement.setString(oneBasedIndex++, location_group_id);
         setIntParameter(statement, oneBasedIndex++, drop_off_type);
         setIntParameter(statement, oneBasedIndex++, pickup_type);
         setIntParameter(statement, oneBasedIndex++, timepoint);
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
}

