package com.conveyal.gtfs.stats.model;

import com.conveyal.gtfs.model.StopTime;
import org.mapdb.Fun;

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
    public Set<Fun.Tuple2<StopTime, StopTime>> missedOpportunities;

    /**
     *
     * @param fromRoute
     * @param toRoute
     * @param minWaitTime
     * @param maxWaitTime
     * @param avgWaitTime
     * @param missedTransfers
     */
    public TransferPerformanceSummary (String fromRoute, String toRoute, int minWaitTime, int maxWaitTime, int avgWaitTime, Set<Fun.Tuple2<StopTime, StopTime>> missedTransfers) {
        this.fromRoute = fromRoute;
        this.toRoute = toRoute;
        bestCase = minWaitTime;
        worstCase = maxWaitTime;
        typicalCase = avgWaitTime;
        missedOpportunities = missedTransfers;
    }

    public String toString () {
        return String.format("From routes %s to %s, the best case transfer time is %d seconds, worst case is %d seconds, and typical case is %d seconds. %d missed near-transfer opportunities.", fromRoute, toRoute, bestCase, worstCase, typicalCase, missedOpportunities.size());
    }

}
