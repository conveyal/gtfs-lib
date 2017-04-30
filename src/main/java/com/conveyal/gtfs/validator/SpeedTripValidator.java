package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;

import java.util.List;

import static com.conveyal.gtfs.error.NewGTFSErrorType.*;
import static com.conveyal.gtfs.util.Util.fastDistance;
import static com.conveyal.gtfs.validator.NewTripTimesValidator.*;

/**
 * Created by abyrd on 2017-04-18
 */
public class SpeedTripValidator extends TripValidator {

    public static final double MIN_SPEED = 0.1;

    public SpeedTripValidator(Feed feed, SQLErrorStorage errorStorage) {
        super(feed, errorStorage);
    }

    @Override
    public void validateTrip(Trip trip, Route route, List<StopTime> stopTimes, List<Stop> stops) {
        // The specific maximum speed for this trip's route's mode of travel.
        double maxSpeed = getMaxSpeed(route);
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
                registerError(DEPARTURE_BEFORE_ARRIVAL, Integer.toString(currStopTime.departure_time), currStopTime);
            }
            double travelTimeSeconds = currStopTime.arrival_time - prevStopTime.departure_time;
            if (checkDistanceAndTime(distanceMeters, travelTimeSeconds, currStopTime)) {
                // If distance and time are OK, we've got valid numbers to calculate a travel speed.
                double metersPerSecond = distanceMeters / travelTimeSeconds;
                if (metersPerSecond < MIN_SPEED || metersPerSecond > maxSpeed) {
                    NewGTFSErrorType type = (metersPerSecond < MIN_SPEED) ? TRAVEL_TOO_SLOW : TRAVEL_TOO_FAST;
                    double threshold = (metersPerSecond < MIN_SPEED) ? MIN_SPEED : maxSpeed;
                    String badValue = String.format("distance=%.0f meters; time=%.0f seconds; speed=%.1f m/sec; threshold=%.0f m/sec",
                            distanceMeters, travelTimeSeconds, metersPerSecond, threshold);
                    registerError(type, badValue, prevStopTime, currStopTime);
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
     * This just pulls some of the range checking logic out of the main trip checking loop so it's more readable.
     * @return true if all values are OK
     */
    private boolean checkDistanceAndTime (double distanceMeters, double travelTimeSeconds, StopTime stopTime) {
        boolean good = true;
        if (distanceMeters == 0) {
            registerError(TRAVEL_DISTANCE_ZERO, null, stopTime);
            good = false;
        }
        if (travelTimeSeconds < 0) {
            registerError(TRAVEL_TIME_NEGATIVE, Double.toString(travelTimeSeconds), stopTime);
            good = false;
        } else if (travelTimeSeconds == 0) {
            registerError(TRAVEL_TIME_ZERO, null, stopTime);
            good = false;
        }
        return good;
    }

    /**
     * @return max speed in meters/second.
     */
    private double getMaxSpeed (Route route) {
        int type = -1;
        if (route != null) type = route.route_type;
        switch (type) {
            case Route.SUBWAY:
                return 40; // 40 m/sec is about 140 kph, speed of HK airport line
            case Route.RAIL:
                return 84; // HSR max speed is around 300kph, about 84 m/sec. *Chinese HSR runs at about 310kph.
            case Route.FERRY:
                return 30; // World's fastest ferry is 107kph or 30 m/sec
            default:
                return 36; // 130 kph is max highway speed, about 36 m/sec
        }
    }


}
