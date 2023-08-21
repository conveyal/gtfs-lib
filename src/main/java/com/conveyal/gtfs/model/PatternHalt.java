package com.conveyal.gtfs.model;

public abstract class PatternHalt extends Entity {
    public String pattern_id;
    public int stop_sequence;
    public abstract int getTravelTime();
    public abstract int getDwellTime();
    public abstract boolean isFlex();
}
