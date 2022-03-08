package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.BookingRule;
import com.conveyal.gtfs.model.FareRule;
import com.conveyal.gtfs.model.Location;
import com.conveyal.gtfs.model.LocationGroup;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopTime;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

import static com.conveyal.gtfs.model.Entity.DOUBLE_MISSING;
import static com.conveyal.gtfs.model.Entity.INT_MISSING;
import static com.conveyal.gtfs.util.GeoJsonUtil.GEOMETRY_TYPE_POLYGON;

/**
 * Spec validation checks for flex additions as defined here:
 * https://github.com/MobilityData/gtfs-flex/blob/master/spec/reference.md
 *
 * Number of checks:
 *
 * Location group: 2
 * Location: 2
 * Stop times: 14
 * Booking rules: 10
 */
public class FlexValidator extends FeedValidator {

    public FlexValidator(Feed feed, SQLErrorStorage errorStorage) {
        super(feed, errorStorage);
    }

    @Override
    public void validate() {
        List<BookingRule> bookingRules = Lists.newArrayList(feed.bookingRules);
        List<LocationGroup> locationGroups = Lists.newArrayList(feed.locationGroups);
        List<Location> locations = Lists.newArrayList(feed.locations);

        if (isFlexFeed(bookingRules, locationGroups, locations)) {
            List<Stop> stops = Lists.newArrayList(feed.stops);
            List<FareRule> fareRules = Lists.newArrayList(feed.fareRules);
            List<NewGTFSError> errors = new ArrayList<>();
            feed.bookingRules.forEach(bookingRule -> errors.addAll(validateBookingRule(bookingRule)));
            feed.locationGroups.forEach(locationGroup -> errors.addAll(validateLocationGroup(locationGroup, stops, locations)));
            feed.locations.forEach(location -> errors.addAll(validateLocation(location, stops, fareRules)));
            feed.stopTimes.forEach(stopTime -> errors.addAll(validateStopTime(stopTime, locationGroups, locations)));
            // Register errors, if any, once all checks have been completed.
            errors.forEach(this::registerError);
        }
    }

    /**
     * Determine if the feed is flex.
     */
    private static boolean isFlexFeed(
        List<BookingRule> bookingRules,
        List<LocationGroup> locationGroups,
        List<Location> locations
    ) {
        return
            (bookingRules != null && !bookingRules.isEmpty()) ||
            (locationGroups != null && !locationGroups.isEmpty()) ||
            (locations != null && !locations.isEmpty());
    }

    /**
     * Check location group id conforms to flex specification constraints.
     */
    public static List<NewGTFSError> validateLocationGroup(
        LocationGroup locationGroup,
        List<Stop> stops,
        List<Location> locations
    ) {
        List<NewGTFSError> errors = new ArrayList<>();
        if (locationGroupIdOrLocationIdIsStopId(stops, locationGroup.location_group_id)) {
            errors.add(NewGTFSError.forEntity(
                locationGroup,
                NewGTFSErrorType.FLEX_FORBIDDEN_LOCATION_GROUP_ID).setBadValue(locationGroup.location_group_id)
            );
        }
        if (locationGroupIdIsLocationId(locations, locationGroup.location_group_id)) {
            errors.add(NewGTFSError.forEntity(
                locationGroup,
                NewGTFSErrorType.FLEX_FORBIDDEN_LOCATION_GROUP_ID).setBadValue(locationGroup.location_group_id)
            );
        }
        return errors;
    }

    /**
     * Check location id and zone id conforms to flex specification constraints.
     */
    public static List<NewGTFSError> validateLocation(Location location, List<Stop> stops, List<FareRule> fareRules) {
        List<NewGTFSError> errors = new ArrayList<>();
        if (locationGroupIdOrLocationIdIsStopId(stops, location.location_id)) {
            // Location id must not match a stop id.
            errors.add(NewGTFSError.forEntity(
                location,
                NewGTFSErrorType.FLEX_FORBIDDEN_LOCATION_ID).setBadValue(location.location_id)
            );
        }
        if (hasFareRules(fareRules, location.zone_id)) {
            // zone id is required if fare rules are defined.
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
        List<LocationGroup> locationGroups,
        List<Location> locations) {

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

        boolean stopIdRefersToLocationGroupOrLocation = stopIdIsLocationGroupOrLocation(
            stopTime.stop_id,
            locationGroups,
            locations
        );

        if (stopTime.start_pickup_dropoff_window == INT_MISSING && stopIdRefersToLocationGroupOrLocation) {
            // start_pickup_dropoff_window is required if stop_id refers to a location group or location.
            errors.add(NewGTFSError.forEntity(
                    stopTime,
                    NewGTFSErrorType.FLEX_REQUIRED_START_PICKUP_DROPOFF_WINDOW)
                .setBadValue(Integer.toString(stopTime.start_pickup_dropoff_window))
            );
        }
        if (stopTime.end_pickup_dropoff_window == INT_MISSING && stopIdRefersToLocationGroupOrLocation) {
            // end_pickup_dropoff_window is required if stop_id refers to a location group or location.
            errors.add(NewGTFSError.forEntity(
                    stopTime,
                    NewGTFSErrorType.FLEX_REQUIRED_END_PICKUP_DROPOFF_WINDOW)
                .setBadValue(Integer.toString(stopTime.end_pickup_dropoff_window))
            );
        }
        if (stopTime.pickup_type == 0 && stopIdRefersToLocationGroupOrLocation) {
            // pickup_type 0 (Regularly scheduled pickup) is forbidden if stop_id refers to a location group or location.
            errors.add(NewGTFSError.forEntity(
                    stopTime,
                    NewGTFSErrorType.FLEX_FORBIDDEN_PICKUP_TYPE)
                .setBadValue(Integer.toString(stopTime.pickup_type))
            );
        }

        boolean stopIdRefersToLocationGroup = stopIdIsLocationGroup(stopTime.stop_id, locationGroups);

        if (stopTime.pickup_type == 3 && stopIdRefersToLocationGroup) {
            // pickup_type 3 (Must coordinate with driver to arrange pickup) is forbidden if stop_id refers to a
            // location group.
            errors.add(NewGTFSError.forEntity(
                    stopTime,
                    NewGTFSErrorType.FLEX_FORBIDDEN_PICKUP_TYPE_FOR_LOCATION_GROUP)
                .setBadValue(Integer.toString(stopTime.pickup_type))
            );
        }

        if (stopTime.pickup_type == 3 && stopIdIsNonPolygonLocation(stopTime.stop_id, locations)) {
            // pickup_type 3 (Must coordinate with driver to arrange pickup) is forbidden if stop_id refers to a
            // location that is not a single "LineString".
            errors.add(NewGTFSError.forEntity(
                    stopTime,
                    NewGTFSErrorType.FLEX_FORBIDDEN_PICKUP_TYPE_FOR_LOCATION)
                .setBadValue(Integer.toString(stopTime.pickup_type))
            );
        }
        if (stopTime.drop_off_type == 0 && stopIdRefersToLocationGroupOrLocation) {
            // drop_off_type 0 (Regularly scheduled pickup) is forbidden if stop_id refers to a location group or location.
            errors.add(NewGTFSError.forEntity(
                    stopTime,
                    NewGTFSErrorType.FLEX_FORBIDDEN_DROP_OFF_TYPE)
                .setBadValue(Integer.toString(stopTime.drop_off_type))
            );
        }
        if (stopTime.mean_duration_factor != DOUBLE_MISSING && !stopIdRefersToLocationGroupOrLocation) {
            // mean_duration_factor is forbidden if stop_id does not refer to a location group or location.
            errors.add(NewGTFSError.forEntity(
                    stopTime,
                    NewGTFSErrorType.FLEX_FORBIDDEN_MEAN_DURATION_FACTOR)
                .setBadValue(Double.toString(stopTime.mean_duration_factor))
            );
        }
        if (stopTime.mean_duration_offset != DOUBLE_MISSING && !stopIdRefersToLocationGroupOrLocation) {
            // mean_duration_offset is forbidden if stop_id does not refer to a location group or location.
            errors.add(NewGTFSError.forEntity(
                    stopTime,
                    NewGTFSErrorType.FLEX_FORBIDDEN_MEAN_DURATION_OFFSET)
                .setBadValue(Double.toString(stopTime.mean_duration_offset))
            );
        }
        if (stopTime.safe_duration_factor != DOUBLE_MISSING && !stopIdRefersToLocationGroupOrLocation) {
            // safe_duration_factor is forbidden if stop_id does not refer to a location group or location.
            errors.add(NewGTFSError.forEntity(
                    stopTime,
                    NewGTFSErrorType.FLEX_FORBIDDEN_SAFE_DURATION_FACTOR)
                .setBadValue(Double.toString(stopTime.safe_duration_factor))
            );
        }
        if (stopTime.safe_duration_offset != DOUBLE_MISSING && !stopIdRefersToLocationGroupOrLocation) {
            // safe_duration_offset is forbidden if stop_id does not refer to a location group or location.
            errors.add(NewGTFSError.forEntity(
                    stopTime,
                    NewGTFSErrorType.FLEX_FORBIDDEN_SAFE_DURATION_OFFSET)
                .setBadValue(Double.toString(stopTime.safe_duration_offset))
            );
        }
        return errors;
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
        if (bookingRule.prior_notice_start_time == INT_MISSING && bookingRule.prior_notice_start_day != INT_MISSING) {
            // prior_notice_start_time is required if prior_notice_start_day is defined.
            errors.add(NewGTFSError.forEntity(
                    bookingRule,
                    NewGTFSErrorType.FLEX_REQUIRED_PRIOR_NOTICE_START_TIME)
                .setBadValue(Integer.toString(bookingRule.prior_notice_start_time))
            );
        }
        if (bookingRule.prior_notice_start_time != INT_MISSING && bookingRule.prior_notice_start_day == INT_MISSING) {
            // prior_notice_start_time is forbidden if prior_notice_start_day is not defined.
            errors.add(NewGTFSError.forEntity(
                    bookingRule,
                    NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_START_TIME)
                .setBadValue(Integer.toString(bookingRule.prior_notice_start_time))
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
     * Check if a location group id or location id matches any stop ids.
     */
    private static boolean locationGroupIdOrLocationIdIsStopId(List<Stop> stops, String id) {
        return !stops.isEmpty() && stops.stream().anyMatch(stop -> stop.stop_id.equals(id));
    }

    /**
     * Check if a location group id matches any location ids.
     */
    private static boolean locationGroupIdIsLocationId(List<Location> locations, String locationGroupId) {
        return !locations.isEmpty() &&
            locations.stream().anyMatch(location -> location.location_id.equals(locationGroupId));
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
     * Check if a stop id matches any location ids or any location group ids.
     */
    private static boolean stopIdIsLocationGroupOrLocation(
        String stopId,
        List<LocationGroup> locationGroups,
        List<Location> locations
    ) {
        return
            ((locationGroups != null && !locationGroups.isEmpty() &&
                locationGroups.stream().anyMatch(locationGroup -> stopId.equals(locationGroup.location_group_id)))
                ||
                (locations != null && !locations.isEmpty() &&
                    locations.stream().anyMatch(location -> stopId.equals(location.location_id))));
    }

    /**
     * Check if a stop id matches any location group ids.
     */
    private static boolean stopIdIsLocationGroup(String stopId, List<LocationGroup> locationGroups) {
        return
            locationGroups != null &&
                !locationGroups.isEmpty() &&
                locationGroups.stream().anyMatch(locationGroup -> stopId.equals(locationGroup.location_group_id));
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
}
