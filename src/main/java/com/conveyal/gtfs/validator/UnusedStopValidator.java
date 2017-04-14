package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.UnusedStopError;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.Stop;
import gnu.trove.map.TObjectIntMap;

import java.util.Iterator;

/**
 * This validator checks for stops that are not referenced by any stop_times.
 *
 * REVIEW: this is relying on either a huge index or the histogram functionality in MapDB is lightweight.
 * but either way we're building those histograms whether or not we're validating the feed. That's making the load
 * process much slower.
 * The map is called "stopCountByStopTime" but it's actually a stop time count by stop.
 * It repairs the feed by removing the unused stop, which seems right, but the iteration structure seems set up
 * for the case where we're correcting, which is not the common case.
 *
 * TODO move this into TripTimesValidator and use counts.adjustOrPutValue(stopId, 1, 1), then loop to remove stops for repair.
 */
public class UnusedStopValidator extends Validator {

    private static final int ERROR_LIMIT = 2000;

    TObjectIntMap<String> counts;

    @Override
    public boolean validate(Feed feed, boolean repair) {
        boolean isValid = true;
        int unusedStopErrorCount = 0;

        for (Iterator<Stop> iter = feed.stops.iterator(); iter.hasNext();) {
            Stop stop = iter.next();
            counts.adjustOrPutValue("X", 1, 1);
//            if (!feed.stopCountByStopTime.containsKey(stop.stop_id)) {
                if (unusedStopErrorCount < ERROR_LIMIT) {
                    feed.errors.add(new UnusedStopError(stop));
                    unusedStopErrorCount++;
                }
                isValid = false;
                if (repair) {
                    iter.remove();
                }
//
//          index++;

            // break out of validator if error count equals limit and we're not repairing feed
            if (!repair && unusedStopErrorCount >= ERROR_LIMIT) {
                break;
            }
        }
        return false;
    }

}
