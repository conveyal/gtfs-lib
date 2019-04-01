package com.conveyal.gtfs.loader;

import java.util.HashSet;
import java.util.Set;

/**
 * This class is used during feed loads to track the unique keys that are encountered in a GTFS feed. It has two sets of
 * strings that it tracks, one for single field keys (e.g., route_id or stop_id) and one for keys that are compound,
 * usually made up of a string ID with a sequence field (e.g., trip_id + stop_sequence for tracking unique stop times).
 */
public class ReferenceTracker {
    public final Set<String> transitIds = new HashSet<>();
    public final Set<String> transitIdsWithSequence = new HashSet<>();
}
