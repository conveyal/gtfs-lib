package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.error.UnusedStopError;
import com.conveyal.gtfs.model.Stop;

import java.util.Iterator;

/**
 * Created by landon on 5/2/16.
 */
public class UnusedStopValidator extends GTFSValidator {
    @Override
    public boolean validate(GTFSFeed feed, boolean repair) {
        boolean isValid = true;
        long index = 1;
        int unusedStopErrorCount = 0;
        int errorLimit = 2000;
        // check for unused stops
        for (Iterator<Stop> iter = feed.stops.values().iterator(); iter.hasNext();) {
            Stop stop = iter.next();
            if (!feed.stopCountByStopTime.containsKey(stop.stop_id)) {
                if (unusedStopErrorCount < errorLimit) {
                    feed.errors.add(new UnusedStopError(stop));
                    unusedStopErrorCount++;
                }
                isValid = false;
                if (repair) {
                    iter.remove();
                }
            }
            index++;

            // break out of validator if error count equals limit and we're not repairing feed
            if (!repair && unusedStopErrorCount >= errorLimit) {
                break;
            }
        }
        return isValid;
    }
}
