package com.conveyal.gtfs.model;

public class PatternStop extends Entity {
    private static final long serialVersionUID = 1L;

    public String pattern_id;
    public int stop_sequence;
    public String stop_id;
    public int default_travel_time;
    public int default_dwell_time;
    public double shape_dist_traveled;
    public int pickup_type;
    public int drop_off_type;
    public int timepoint;

    /**
     * Construct a pattern stop using information from an exemplar trip (usually the first trip).
     *
     * FIXME: Should we be storing default travel and dwell times here?
     */
    public PatternStop (String pattern_id, String stop_id, int stop_sequence, int default_travel_time,
                        int default_dwell_time, double shape_dist_traveled, int pickup_type, int drop_off_type) {
        this.pattern_id = pattern_id;
        this.stop_id = stop_id;
        this.shape_dist_traveled = shape_dist_traveled;
        this.stop_sequence = stop_sequence;
        this.default_travel_time = default_travel_time;
        this.default_dwell_time = default_dwell_time;
        this.pickup_type = pickup_type;
        this.drop_off_type = drop_off_type;
    }
}
