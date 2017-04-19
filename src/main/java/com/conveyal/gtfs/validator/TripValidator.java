package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;

import java.util.List;

/**
 * Unlike FeedValidators that are run against the entire feed, these validators are run against the stop_times for
 * a specific trip. This is an optimization that allows us to fetch and group those stop_times only once.
 */
public abstract class TripValidator extends Validator {

    /**
     * This method will be called on each trip in the feed.
     * @param feed
     * @param trip the trip whose stop_times are provided in the other parameter.
     * @param stopTimes a List of all the stop times in the given trip, in order of increasing stop_sequence.
     */
    public abstract void validateTrip(Feed feed, Trip trip, List<StopTime> stopTimes);

    /**
     * This method will be called after validateTrip has been called on the last trip in the feed.
     * This allows the TripValidator implementation to perform any analysis or checking that uses information
     * accumulated across all the trips.
     */
    public List<NewGTFSError> complete () { return errors; }


}
