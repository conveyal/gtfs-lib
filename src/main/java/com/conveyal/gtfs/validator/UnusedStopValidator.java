package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.error.UnusedStopError;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.validator.model.InvalidValue;
import com.conveyal.gtfs.validator.model.Priority;
import com.conveyal.gtfs.validator.model.ValidationResult;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by landon on 5/2/16.
 */
public class UnusedStopValidator extends GTFSValidator {
    @Override
    public boolean validate(GTFSFeed feed, boolean repair) {
        boolean isValid = true;
        ValidationResult result = new ValidationResult();
        long index = 1;
        // check for unused stops
        for (Iterator<Stop> iter = feed.stops.values().iterator(); iter.hasNext();) {
            Stop stop = iter.next();
            if(feed.getStopTimesForStop(stop.stop_id).isEmpty()) {
                feed.errors.add(new UnusedStopError(stop.stop_id, index, stop));
                isValid = false;
                if (repair) {
                    iter.remove();
                }
            }
            index++;
        }
        return isValid;
    }
}
