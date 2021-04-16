package com.conveyal.gtfs.loader;

/**
 * These are the values that are checked inline with {@link ConditionallyRequiredCheck} to determine if the required
 * conditions have been met.
 */
public class ConditionallyRequired {
    /** The type of check to be carried out */
    public final ConditionallyRequiredCheck check;
    /** The minimum column value if a range check is being performed. */
    public double minValue;
    /** The maximum column value if a range check is being performed. */
    public double maxValue;

    ConditionallyRequired(ConditionallyRequiredCheck check, double minValue, double maxValue) {
        this.check = check;
        this.minValue = minValue;
        this.maxValue = maxValue;
    }
}
