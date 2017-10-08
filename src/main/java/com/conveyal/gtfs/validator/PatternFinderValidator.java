package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.PatternFinder;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;

import java.util.List;

/**
 * Groups trips together into "patterns" that share the same sequence of stops.
 * This is not a normal validator in the sense that it does not check for bad data.
 * It's taking advantage of the fact that we're already iterating over the trips one by one to build up the patterns.
 */
public class PatternFinderValidator extends TripValidator {

    PatternFinder patternFinder;

    public PatternFinderValidator(Feed feed, SQLErrorStorage errorStorage) {
        super(feed, errorStorage);
        patternFinder = new PatternFinder();
    }

    @Override
    public void validateTrip (Trip trip, Route route, List<StopTime> stopTimes, List<Stop> stops) {
        // As we hit each trip, accumulate them into the wrapped PatternFinder object.
        patternFinder.processTrip(trip, stopTimes);
    }

    @Override
    public void complete(ValidationResult validationResult) {
        // Return patterns in the result
        // TODO should we really do this or just have them in the DB?
        validationResult.patterns = patternFinder.createPatternObjects();

    }

}

