package com.conveyal.gtfs.loader;

import java.util.HashSet;
import java.util.Set;

public class ReferenceTracker {
    public final Set<String> transitIds = new HashSet<>();
    public final Set<String> transitIdsWithSequence = new HashSet<>();
}
