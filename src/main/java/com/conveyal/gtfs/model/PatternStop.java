package com.conveyal.gtfs.model;

public class PatternStop extends Entity {
    private static final long serialVersionUID = 1L;

    public String pattern_id;
    public String stop_id;
    public int stop_sequence;
    public PatternStop () {}
}
