package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.error.UnusedStopError;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.validator.model.InvalidValue;
import com.conveyal.gtfs.validator.model.Priority;
import com.conveyal.gtfs.validator.model.ValidationResult;

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

        List<String> usedStopIds = feed.stop_times.values().stream().map(stopTime -> stopTime.stop_id).collect(Collectors.toList());

        // check for unused stops
        for(Stop stop : feed.stops.values()) {
            if(!usedStopIds.contains(stop.stop_id)) {
                feed.errors.add(new UnusedStopError(stop.stop_id, stop, Priority.LOW));
                result.add(new InvalidValue("stop", "stop_id", stop.stop_id, "UnusedStop", "Stop Id " + stop.stop_id + " is not used in any trips." , null, Priority.LOW));
                isValid = false;
            }
        }
        return isValid;
    }
}
