package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.conveyal.gtfs.error.NewGTFSErrorType.*;
import static com.conveyal.gtfs.util.Util.fastDistance;
import static com.conveyal.gtfs.validator.NewTripTimesValidator.*;

/**
 * Created by abyrd on 2017-04-18
 */
public class SpeedTripValidator extends TripValidator {

    public static final double MIN_SPEED_KPH = 0.5;
    private boolean allTravelTimesAreRounded = true;
    private Set<NewGTFSError> travelTimeZeroErrors = new HashSet<>();

    public SpeedTripValidator(Feed feed, SQLErrorStorage errorStorage) {
        super(feed, errorStorage);
    }

    @Override
    public void validateTrip(Trip trip, Route route, List<StopTime> stopTimes, List<Stop> stops) {
        // The specific maximum speed for this trip's route's mode of travel.
        double maxSpeedKph = getMaxSpeedKph(route);
        // Skip over any initial stop times that won't allow calculating speeds.
        int beginIndex = 0;
        while (missingBothTimes(stopTimes.get(beginIndex))) {
            beginIndex++;
            if (beginIndex == stopTimes.size()) return;
        }
        // Unfortunately we can't work on each stop pair in isolation,
        // because we want to accumulate distance when stop times are missing.
        StopTime prevStopTime = stopTimes.get(beginIndex);
        Stop prevStop = stops.get(beginIndex);
        double distanceMeters = 0;
        for (int i = beginIndex + 1; i < stopTimes.size(); i++) {
            StopTime currStopTime = stopTimes.get(i);
            Stop currStop = stops.get(i);
            // Distance is accumulated in case times are not provided for some StopTimes.
            distanceMeters += fastDistance(currStop.stop_lat, currStop.stop_lon, prevStop.stop_lat, prevStop.stop_lon);
            if (missingBothTimes(currStopTime)) {
                // FixMissingTimes has already been called, so both arrival and departure time are missing.
                // The spec allows this. Other than accumulating distance, skip this StopTime.
                continue;
            }
            if (currStopTime.departure_time < currStopTime.arrival_time) {
                registerError(currStopTime, DEPARTURE_BEFORE_ARRIVAL);
            }
            // Detect if travel times are rounded off to minutes.
            boolean bothTravelTimesRounded = areTravelTimesRounded(prevStopTime);
            double travelTimeSeconds = currStopTime.arrival_time - prevStopTime.departure_time;
            // If travel times are rounded and travel time is zero, determine the maximum and minimum possible speed
            // by adding/removing one minute of slack.
            if (bothTravelTimesRounded && travelTimeSeconds == 0)
                travelTimeSeconds += 60;
            if (checkDistanceAndTime(distanceMeters, travelTimeSeconds, currStopTime)) {
                // If distance and time are OK, we've got valid numbers to calculate a travel speed.
                double kph = (distanceMeters / 1000D) / (travelTimeSeconds / 60D / 60D);
                if (kph < MIN_SPEED_KPH) {
                    registerError(currStopTime, TRAVEL_TOO_SLOW, String.format("%2.1f km/h", kph));
                } else if (kph > maxSpeedKph) {
                    registerError(currStopTime, TRAVEL_TOO_FAST, String.format("%2.1f km/h", kph));
                }
            }
            // Reset accumulated distance, we've processed a stop time with arrival or departure time specified.
            distanceMeters = 0;
            // Record current stop and stopTime for the next iteration.
            prevStopTime = currStopTime;
            prevStop = currStop;
        }
    }

    /**
     * Completing this feed validator means checking if there were any unrounded travel times in the feed and (if so)
     * registering any zero travel time errors that were passed over before the first unrounded travel time was
     * encountered. If in fact all travel times are rounded to the minute, store a special feed-wide error in this case.
     */
    public void complete (ValidationResult validationResult) {
        if (!allTravelTimesAreRounded) storeErrors(travelTimeZeroErrors);
        else registerError(NewGTFSError.forFeed(FEED_TRAVEL_TIMES_ROUNDED, null));
    }

    /**
     * Check that arrival and departure time for a stop time are rounded to the minute and update
     * {@link #allTravelTimesAreRounded} accordingly.
     */
    private boolean areTravelTimesRounded(StopTime stopTime) {
        boolean bothTravelTimesAreRounded = stopTime.departure_time % 60 == 0 && stopTime.arrival_time % 60 == 0;
        if (!bothTravelTimesAreRounded) this.allTravelTimesAreRounded = false;
        return bothTravelTimesAreRounded;
    }

    /**
     * This just pulls some of the range checking logic out of the main trip checking loop so it's more readable.
     * @return true if all values are OK
     */
    private boolean checkDistanceAndTime (double distanceMeters, double travelTimeSeconds, StopTime stopTime) {
        boolean good = true;
        // TODO Use Epsilon for very tiny travel e.g. < 5 meters
        if (distanceMeters == 0) {
            registerError(stopTime, TRAVEL_DISTANCE_ZERO);
            good = false;
        }
        if (travelTimeSeconds < 0) {
            registerError(stopTime, TRAVEL_TIME_NEGATIVE, travelTimeSeconds);
            good = false;
        } else if (travelTimeSeconds == 0) {
            // Only register the travel time zero error if not all travel times are rounded. Otherwise, hold onto the
            // error in the travelTimeZeroErrors collection until the completion of this validator.
            if (!allTravelTimesAreRounded) registerError(stopTime, TRAVEL_TIME_ZERO);
            else travelTimeZeroErrors.add(createUnregisteredError(stopTime, TRAVEL_TIME_ZERO));
            good = false;
        }
        return good;
    }

    /**
     * @return max speed in km/hour.
     */
    private static double getMaxSpeedKph (Route route) {
        int type = -1;
        if (route != null) type = route.route_type;
        switch (type) {
            case Route.SUBWAY:
                return 140; // Speed of HK airport line.
            case Route.RAIL:
                return 310; // European HSR max speed is around 300kph, Chinese HSR runs at about 310kph.
            case Route.FERRY:
                return 107; // World's fastest ferry is 107kph.
            default:
                return 130; // 130 kph is max highway speed.
        }
    }


}
