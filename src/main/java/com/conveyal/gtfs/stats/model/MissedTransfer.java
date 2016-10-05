package com.conveyal.gtfs.stats.model;

import java.util.Set;

/**
 * Created by landon on 10/4/16.
 */
public class MissedTransfer {
    public Set<String> tripIds;

    public MissedTransfer (Set<String> tripIds) {
        this.tripIds = tripIds;
    }
}
