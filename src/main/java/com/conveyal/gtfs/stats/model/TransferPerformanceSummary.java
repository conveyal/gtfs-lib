package com.conveyal.gtfs.stats.model;

import com.conveyal.gtfs.model.StopTime;
import java.util.Set;

/**
 * Created by landon on 10/4/16.
 */
public class TransferPerformanceSummary {
    public String fromRoute;
    public String toRoute;
    public int bestCase;
    public int worstCase;
    public int typicalCase;
    public Set<Set<StopTime>> missedOpportunities;
    public TransferPerformanceSummary (String fromRoute, String toRoute, int minWaitTime, int maxWaitTime, int avgWaitTime, Set<Set<StopTime>> missedTransfers) {
        this.fromRoute = fromRoute;
        this.toRoute = toRoute;
        bestCase = minWaitTime;
        worstCase = maxWaitTime;
        typicalCase = avgWaitTime;
        missedOpportunities = missedTransfers;
    }


}
