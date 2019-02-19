package com.conveyal.gtfs.loader;

/**
 * This class contains a convenient way to describe illegal character sequences during GTFS feed loading, as well as the
 * appropriate replacement value and a human-readable description (rather than inputting the bad sequence into the
 * database and having to deal with downstream issues).
 */
public class IllegalCharacter {
    public String illegalSequence;
    public String replacement;
    public String description;

    public IllegalCharacter(String illegalSequence, String replacement, String description) {
        this.illegalSequence = illegalSequence;
        this.replacement = replacement;
        this.description = description;
    }
}
