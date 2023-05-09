    package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.BookingRule;
import com.conveyal.gtfs.model.FareRule;
import com.conveyal.gtfs.model.Location;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopArea;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import com.google.common.collect.Lists;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static com.conveyal.gtfs.error.NewGTFSErrorType.VALIDATOR_FAILED;
import static com.conveyal.gtfs.model.Entity.DOUBLE_MISSING;
import static com.conveyal.gtfs.model.Entity.INT_MISSING;
import static com.conveyal.gtfs.model.StopTime.getFlexStopTimesForValidation;
import static com.conveyal.gtfs.util.GeoJsonUtil.GEOMETRY_TYPE_POLYGON;

/**
 * Spec validation checks for flex additions as defined here:
 * https://github.com/MobilityData/gtfs-flex/blob/master/spec/reference.md
 *
 * Number of checks:
 *
 * Stop area: 2
 * Location: 2
 * Stop times: 14
 * Booking rules: 10
 */
public class FlexValidator extends FeedValidator {

    DataSource dataSource;

    public FlexValidator(Feed feed, SQLErrorStorage errorStorage, DataSource dataSource) {
        super(feed, errorStorage);
        this.dataSource = dataSource;
    }

    @Override
    public void validate() {
        List<BookingRule> bookingRules = Lists.newArrayList(feed.bookingRules);
        List<StopArea> stopAreas = Lists.newArrayList(feed.stopAreas);
        List<Location> locations = Lists.newArrayList(feed.locations);

        if (isFlexFeed(bookingRules, stopAreas, locations)) {
            List<NewGTFSError> errors = new ArrayList<>();
            try {
                List<StopTime> stopTimes = getFlexStopTimesForValidation(dataSource.getConnection(), feed.tablePrefix);
                stopTimes.forEach(stopTime -> errors.addAll(validateStopTime(stopTime, stopAreas, locations)));
                feed.trips.forEach(trip -> errors.addAll(validateTrip(trip, stopTimes, stopAreas, locations)));
            } catch (SQLException e) {
                String badValue = String.join(":", this.getClass().getSimpleName(), e.toString());
                errorStorage.storeError(NewGTFSError.forFeed(VALIDATOR_FAILED, badValue));
            }
            List<Stop> stops = Lists.newArrayList(feed.stops);
            List<FareRule> fareRules = Lists.newArrayList(feed.fareRules);
            feed.bookingRules.forEach(bookingRule -> errors.addAll(validateBookingRule(bookingRule)));
            feed.stopAreas.forEach(stopArea -> errors.addAll(validateStopArea(stopArea, stops, locations)));
            feed.locations.forEach(location -> errors.addAll(validateLocation(location, stops, fareRules)));
            // Register errors, if any, once all checks have been completed.
            errors.forEach(this::registerError);
        }
    }

    /**
     * Determine if the feed is flex.
     */
    private static boolean isFlexFeed(
        List<BookingRule> bookingRules,
        List<StopArea> stopAreas,
        List<Location> locations
    ) {
        return
            (bookingRules != null && !bookingRules.isEmpty()) ||
            (stopAreas != null && !stopAreas.isEmpty()) ||
            (locations != null && !locations.isEmpty());
    }

    /**
     * Check if a trip contains a stop that references a location or stop area. A trip's speed can not be validated
     * if at least one stop references a location or stop area.
     */
    public static List<NewGTFSError> validateTrip(
        Trip trip,
        List<StopTime> stopTimes,
        List<StopArea> stopAreas,
        List<Location> locations
    ) {
        List<NewGTFSError> errors = new ArrayList<>();
        if (tripHasStopAreaOrLocationForStop(trip, stopTimes, stopAreas, locations)) {
            errors.add(NewGTFSError.forEntity(
                trip,
                NewGTFSErrorType.TRIP_SPEED_NOT_VALIDATED).setBadValue(trip.trip_id)
            );
        }
        return errors;
    }

    /**
     * Check stop area's area id conforms to flex specification constraints.
     */
    public static List<NewGTFSError> validateStopArea(
        StopArea stopArea,
        List<Stop> stops,
        List<Location> locations
    ) {
        List<NewGTFSError> errors = new ArrayList<>();
        if (
            stopAreaOrLocationIsStop(stops, stopArea.area_id) ||
            stopAreaIsLocation(locations, stopArea.area_id)
        ) {
            errors.add(NewGTFSError.forEntity(
                stopArea,
                NewGTFSErrorType.FLEX_FORBIDDEN_STOP_AREA_AREA_ID).setBadValue(stopArea.area_id)
            );
        }
        return errors;
    }

    /**
     * Check location id and zone id conforms to flex specification constraints.
     */
    public static List<NewGTFSError> validateLocation(Location location, List<Stop> stops, List<FareRule> fareRules) {
        List<NewGTFSError> errors = new ArrayList<>();
        if (stopAreaOrLocationIsStop(stops, location.location_id)) {
            errors.add(NewGTFSError.forEntity(
                location,
                NewGTFSErrorType.FLEX_FORBIDDEN_LOCATION_ID).setBadValue(location.location_id)
            );
        }
        if (hasFareRules(fareRules, location.zone_id)) {
            errors.add(NewGTFSError.forEntity(
                location,
                NewGTFSErrorType.FLEX_MISSING_FARE_RULE).setBadValue(location.zone_id)
            );
        }
        return errors;
    }

    /**
     * Check that a stop time conforms to flex specification constraints.
     */
    public static List<NewGTFSError> validateStopTime(
        StopTime stopTime,
        List<StopArea> stopAreas,
        List<Location> locations
    ) {

        List<NewGTFSError> errors = new ArrayList<>();

        if (stopTime.arrival_time != INT_MISSING &&
            (stopTime.start_pickup_dropoff_window != INT_MISSING ||
                stopTime.end_pickup_dropoff_window != INT_MISSING)
        ) {
            // Arrival time must not be defined if start/end pickup drop off window is defined.
            errors.add(NewGTFSError.forEntity(
                stopTime,
                NewGTFSErrorType.FLEX_FORBIDDEN_ARRIVAL_TIME).setBadValue(Integer.toString(stopTime.arrival_time))
            );
        }
        if (stopTime.departure_time != INT_MISSING &&
            (stopTime.start_pickup_dropoff_window != INT_MISSING ||
                stopTime.end_pickup_dropoff_window != INT_MISSING)
        ) {
            // Departure time must not be defined if start/end pickup drop off window is defined.
            errors.add(NewGTFSError.forEntity(
                stopTime,
                NewGTFSErrorType.FLEX_FORBIDDEN_DEPARTURE_TIME).setBadValue(Integer.toString(stopTime.departure_time))
            );
        }
        if (stopTime.start_pickup_dropoff_window != INT_MISSING &&
            (stopTime.arrival_time != INT_MISSING ||
                stopTime.departure_time != INT_MISSING)
        ) {
            // start_pickup_dropoff_window is forbidden if arrival time or departure time are defined.
            errors.add(NewGTFSError.forEntity(
                    stopTime,
                    NewGTFSErrorType.FLEX_FORBIDDEN_START_PICKUP_DROPOFF_WINDOW)
                .setBadValue(Integer.toString(stopTime.start_pickup_dropoff_window))
            );
        }
        if (stopTime.end_pickup_dropoff_window != INT_MISSING &&
            (stopTime.arrival_time != INT_MISSING ||
                stopTime.departure_time != INT_MISSING)
        ) {
            // end_pickup_dropoff_window is forbidden if arrival time or departure time are defined.
            errors.add(NewGTFSError.forEntity(
                    stopTime,
                    NewGTFSErrorType.FLEX_FORBIDDEN_END_PICKUP_DROPOFF_WINDOW)
                .setBadValue(Integer.toString(stopTime.end_pickup_dropoff_window))
            );
        }

        boolean stopIdRefersToStopAreaOrLocation = stopIdIsStopAreaOrLocation(
            stopTime.stop_id,
            stopAreas,
            locations
        );

        if (stopTime.start_pickup_dropoff_window == INT_MISSING && stopIdRefersToStopAreaOrLocation) {
            // start_pickup_dropoff_window is required if stop_id refers to a stop area or location.
            errors.add(NewGTFSError.forEntity(
                    stopTime,
                    NewGTFSErrorType.FLEX_REQUIRED_START_PICKUP_DROPOFF_WINDOW)
                .setBadValue(Integer.toString(stopTime.start_pickup_dropoff_window))
            );
        }
        if (stopTime.end_pickup_dropoff_window == INT_MISSING && stopIdRefersToStopAreaOrLocation) {
            // end_pickup_dropoff_window is required if stop_id refers to a stop area or location.
            errors.add(NewGTFSError.forEntity(
                    stopTime,
                    NewGTFSErrorType.FLEX_REQUIRED_END_PICKUP_DROPOFF_WINDOW)
                .setBadValue(Integer.toString(stopTime.end_pickup_dropoff_window))
            );
        }
        if (stopTime.pickup_type == 0 && stopIdRefersToStopAreaOrLocation) {
            // pickup_type 0 (Regularly scheduled pickup) is forbidden if stop_id refers to a stop area or location.
            errors.add(NewGTFSError.forEntity(
                    stopTime,
                    NewGTFSErrorType.FLEX_FORBIDDEN_PICKUP_TYPE)
                .setBadValue(Integer.toString(stopTime.pickup_type))
            );
        }

        boolean stopIdRefersToStopArea = stopIdIsStopArea(stopTime.stop_id, stopAreas);

        if (stopTime.pickup_type == 3 && stopIdRefersToStopArea) {
            // pickup_type 3 (Must coordinate with driver to arrange pickup) is forbidden if stop_id refers to a
            // stop area.
            errors.add(NewGTFSError.forEntity(
                    stopTime,
                    NewGTFSErrorType.FLEX_FORBIDDEN_PICKUP_TYPE_FOR_STOP_AREA)
                .setBadValue(Integer.toString(stopTime.pickup_type))
            );
        }

        if (stopTime.pickup_type == 3 && stopIdIsLocation(stopTime.stop_id, locations)) {
            // pickup_type 3 (Must coordinate with driver to arrange pickup) is forbidden if stop_id refers to a
            // location.
            errors.add(NewGTFSError.forEntity(
                    stopTime,
                    NewGTFSErrorType.FLEX_FORBIDDEN_PICKUP_TYPE_FOR_LOCATION)
                .setBadValue(Integer.toString(stopTime.pickup_type))
            );
        }
        if (stopTime.drop_off_type == 0 && stopIdRefersToStopAreaOrLocation) {
            // drop_off_type 0 (Regularly scheduled pickup) is forbidden if stop_id refers to a stop area or location.
            errors.add(NewGTFSError.forEntity(
                    stopTime,
                    NewGTFSErrorType.FLEX_FORBIDDEN_DROP_OFF_TYPE)
                .setBadValue(Integer.toString(stopTime.drop_off_type))
            );
        }
        if (stopTime.mean_duration_factor != DOUBLE_MISSING && !stopIdRefersToStopAreaOrLocation) {
            // mean_duration_factor is forbidden if stop_id does not refer to a stop area or location.
            errors.add(NewGTFSError.forEntity(
                    stopTime,
                    NewGTFSErrorType.FLEX_FORBIDDEN_MEAN_DURATION_FACTOR)
                .setBadValue(Double.toString(stopTime.mean_duration_factor))
            );
        }
        if (stopTime.mean_duration_offset != DOUBLE_MISSING && !stopIdRefersToStopAreaOrLocation) {
            // mean_duration_offset is forbidden if stop_id does not refer to a stop area or location.
            errors.add(NewGTFSError.forEntity(
                    stopTime,
                    NewGTFSErrorType.FLEX_FORBIDDEN_MEAN_DURATION_OFFSET)
                .setBadValue(Double.toString(stopTime.mean_duration_offset))
            );
        }
        if (stopTime.safe_duration_factor != DOUBLE_MISSING && !stopIdRefersToStopAreaOrLocation) {
            // safe_duration_factor is forbidden if stop_id does not refer to a stop area or location.
            errors.add(NewGTFSError.forEntity(
                    stopTime,
                    NewGTFSErrorType.FLEX_FORBIDDEN_SAFE_DURATION_FACTOR)
                .setBadValue(Double.toString(stopTime.safe_duration_factor))
            );
        }
        if (stopTime.safe_duration_offset != DOUBLE_MISSING && !stopIdRefersToStopAreaOrLocation) {
            // safe_duration_offset is forbidden if stop_id does not refer to a stop area or location.
            errors.add(NewGTFSError.forEntity(
                    stopTime,
                    NewGTFSErrorType.FLEX_FORBIDDEN_SAFE_DURATION_OFFSET)
                .setBadValue(Double.toString(stopTime.safe_duration_offset))
            );
        }

        if (!isSafeFactorGreatThanMeanFactor(stopTime)) {
            errors.add(NewGTFSError.forEntity(
                    stopTime,
                    NewGTFSErrorType.FLEX_SAFE_FACTORS_EXCEEDED)
                .setBadValue(Double.toString(stopTime.safe_duration_offset))
            );
        }

        return errors;
    }

    /**
     * The safe factors must be greater than the mean factors. This includes safe/mean duration offset and
     * factor.
     */
    private static boolean isSafeFactorGreatThanMeanFactor(StopTime stopTime) {
        return stopTime.safe_duration_factor + stopTime.safe_duration_offset >=
            stopTime.mean_duration_factor + stopTime.mean_duration_offset;
    }

    /**
     * Check that a booking rule conforms to flex specification constraints.
     */
    public static List<NewGTFSError> validateBookingRule(BookingRule bookingRule) {
        List<NewGTFSError> errors = new ArrayList<>();

        if (bookingRule.prior_notice_duration_min == INT_MISSING && bookingRule.booking_type == 1) {
            // prior_notice_duration_min is required for booking_type 1 (Up to same-day booking with advance notice).
            errors.add(NewGTFSError.forEntity(
                    bookingRule,
                    NewGTFSErrorType.FLEX_REQUIRED_PRIOR_NOTICE_DURATION_MIN)
                .setBadValue(Integer.toString(bookingRule.prior_notice_duration_min))
            );
        }
        if (bookingRule.prior_notice_duration_min != INT_MISSING && bookingRule.booking_type != 1) {
            // prior_notice_duration_min is forbidden for all but booking_type 1 (Up to same-day booking with advance notice).
            errors.add(NewGTFSError.forEntity(
                    bookingRule,
                    NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_DURATION_MIN)
                .setBadValue(Integer.toString(bookingRule.prior_notice_duration_min))
            );
        }
        if (bookingRule.prior_notice_duration_max != INT_MISSING &&
            (bookingRule.booking_type == 0 || bookingRule.booking_type == 2)
        ) {
            // prior_notice_duration_max is forbidden for booking_type 0 (Real time booking) &
            // 2 (Up to prior day(s) booking).
            errors.add(NewGTFSError.forEntity(
                    bookingRule,
                    NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_DURATION_MAX)
                .setBadValue(Integer.toString(bookingRule.prior_notice_duration_max))
            );
        }
        if (bookingRule.prior_notice_last_day == INT_MISSING && bookingRule.booking_type == 2) {
            // prior_notice_last_day is required for booking_type 2 (Up to prior day(s) booking).
            errors.add(NewGTFSError.forEntity(
                    bookingRule,
                    NewGTFSErrorType.FLEX_REQUIRED_PRIOR_NOTICE_LAST_DAY)
                .setBadValue(Integer.toString(bookingRule.prior_notice_last_day))
            );
        }
        if (bookingRule.prior_notice_last_day != INT_MISSING && bookingRule.booking_type != 2) {
            // prior_notice_last_day is forbidden for all but booking_type 2 (Up to prior day(s) booking).
            errors.add(NewGTFSError.forEntity(
                    bookingRule,
                    NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_LAST_DAY)
                .setBadValue(Integer.toString(bookingRule.prior_notice_last_day))
            );
        }
        if (bookingRule.prior_notice_start_day != INT_MISSING && bookingRule.booking_type == 0) {
            // prior_notice_start_day is forbidden for booking_type 0 (Real time booking).
            errors.add(NewGTFSError.forEntity(
                    bookingRule,
                    NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_START_DAY_FOR_BOOKING_TYPE)
                .setBadValue(Integer.toString(bookingRule.prior_notice_start_day))
            );
        }
        if (bookingRule.prior_notice_start_day != INT_MISSING &&
            bookingRule.booking_type == 1 &&
            bookingRule.prior_notice_duration_max != INT_MISSING
        ) {
            // prior_notice_start_day is forbidden for booking_type 1 (Up to same-day booking with advance notice) if
            // prior_notice_duration_max is defined.
            errors.add(NewGTFSError.forEntity(
                    bookingRule,
                    NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_START_DAY)
                .setBadValue(Integer.toString(bookingRule.prior_notice_start_day))
            );
        }
        if (bookingRule.prior_notice_start_time == null && bookingRule.prior_notice_start_day != INT_MISSING) {
            // prior_notice_start_time is required if prior_notice_start_day is defined.
            errors.add(NewGTFSError.forEntity(
                    bookingRule,
                    NewGTFSErrorType.FLEX_REQUIRED_PRIOR_NOTICE_START_TIME)
                .setBadValue(bookingRule.prior_notice_start_time)
            );
        }
        if (bookingRule.prior_notice_start_time != null && bookingRule.prior_notice_start_day == INT_MISSING) {
            // prior_notice_start_time is forbidden if prior_notice_start_day is not defined.
            errors.add(NewGTFSError.forEntity(
                    bookingRule,
                    NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_START_TIME)
                .setBadValue(bookingRule.prior_notice_start_time)
            );
        }
        if ((bookingRule.prior_notice_service_id != null &&
            !bookingRule.prior_notice_service_id.equals("")) &&
            bookingRule.booking_type != 2) {
            // prior_notice_service_id is forbidden for all but booking_type 2 (Up to prior day(s) booking).
            errors.add(NewGTFSError.forEntity(
                    bookingRule,
                    NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_SERVICE_ID)
                .setBadValue(bookingRule.prior_notice_service_id)
            );
        }
        return errors;
    }

    /**
     * Check if a stop area or location matches any stop ids.
     */
    private static boolean stopAreaOrLocationIsStop(List<Stop> stops, String id) {
        return !stops.isEmpty() && stops.stream().anyMatch(stop -> stop.stop_id.equals(id));
    }

    /**
     * Check if a stop area (area id) matches any locations.
     */
    private static boolean stopAreaIsLocation(List<Location> locations, String areaId) {
        return !locations.isEmpty() &&
            locations.stream().anyMatch(location -> location.location_id.equals(areaId));
    }

    /**
     * If fare rules are defined, check there is a match on zone id.
     */
    private static boolean hasFareRules(List<FareRule> fareRules, String zoneId) {
        return fareRules != null &&
            !fareRules.isEmpty() &&
            fareRules.stream().anyMatch(fareRule ->
                (fareRule.contains_id != null && fareRule.destination_id != null && fareRule.origin_id != null) &&
                    !fareRule.contains_id.equals(zoneId) &&
                    !fareRule.destination_id.equals(zoneId) &&
                    !fareRule.origin_id.equals(zoneId));
    }

    /**
     * Check if a stop id matches any locations or any stop areas.
     */
    public static boolean stopIdIsStopAreaOrLocation(
        String stopId,
        List<StopArea> stopAreas,
        List<Location> locations
    ) {
        return stopIdIsStopArea(stopId, stopAreas) || stopIdIsLocation(stopId, locations);
    }

    /**
     * Check if a stop id matches any stop area, area ids.
     */
    public static boolean stopIdIsStopArea(String stopId, List<StopArea> stopAreas) {
        return
            stopAreas != null &&
                !stopAreas.isEmpty() &&
                stopAreas.stream().anyMatch(stopArea -> stopId.equals(stopArea.area_id));
    }

    /**
     * Check if a stop id matches any location ids.
     */
    public static boolean stopIdIsLocation(String stopId, List<Location> locations) {
        return
            locations != null &&
                !locations.isEmpty() &&
                locations.stream().anyMatch(location -> stopId.equals(location.location_id));
    }

    /**
     * Check if a stop id refers to a non polygon (LineString) location.
     */
    private static boolean stopIdIsNonPolygonLocation(String stopId, List<Location> locations) {
        return
            locations != null &&
            !locations.isEmpty() &&
            locations.stream().anyMatch(location ->
                stopId.equals(location.location_id) &&
                    !location.geometry_type.equals(GEOMETRY_TYPE_POLYGON)
            );
    }

    /**
     * Check if a trip contains at least one stop time that references a stop that is a location or stop area.
     */
    public static boolean tripHasStopAreaOrLocationForStop(
        Trip trip,
        List<StopTime> stopTimes,
        List<StopArea> stopAreas,
        List<Location> locations
    ) {
        for (StopTime stopTime : stopTimes) {
            if (
                trip.trip_id.equals(stopTime.trip_id) &&
                stopIdIsStopAreaOrLocation(stopTime.stop_id, stopAreas, locations)
            ) {
                return true;
            }
        }
        return false;
    }
}
