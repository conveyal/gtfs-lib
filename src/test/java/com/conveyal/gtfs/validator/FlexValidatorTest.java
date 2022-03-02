package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.model.BookingRule;
import com.conveyal.gtfs.model.FareRule;
import com.conveyal.gtfs.model.Location;
import com.conveyal.gtfs.model.LocationGroup;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopTime;
import com.google.common.collect.Lists;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.conveyal.gtfs.error.NewGTFSErrorType.FLEX_FORBIDDEN_END_PICKUP_DROPOFF_WINDOW;
import static com.conveyal.gtfs.error.NewGTFSErrorType.FLEX_FORBIDDEN_START_PICKUP_DROPOFF_WINDOW;
import static com.conveyal.gtfs.model.Entity.DOUBLE_MISSING;
import static com.conveyal.gtfs.model.Entity.INT_MISSING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FlexValidatorTest {
    private static final FlexValidator flexValidator = new FlexValidator();

    @ParameterizedTest
    @MethodSource("createLocationGroupChecks")
    void validateLocationGroupTests(String locationGroupId, String locationId, String stopId, NewGTFSErrorType expectedError) {
        LocationGroup locationGroup = new LocationGroup();
        locationGroup.location_group_id = locationGroupId;
        flexValidator.errors.clear();
        flexValidator.validateLocationGroups(
            locationGroup,
            Lists.newArrayList(createStop(stopId)),
            Lists.newArrayList(createLocation(locationId))
        );
        checkValidationErrorsMatchExpectedErrors(flexValidator.errors, Lists.newArrayList(expectedError));
    }

    private static Stream<Arguments> createLocationGroupChecks() {
        return Stream.of(
            Arguments.of("1", "1", "2", NewGTFSErrorType.FLEX_FORBIDDEN_LOCATION_GROUP_ID),
            Arguments.of("1", "2", "1", NewGTFSErrorType.FLEX_FORBIDDEN_LOCATION_GROUP_ID),
            Arguments.of("1", "2", "3", null)
        );
    }

    @ParameterizedTest
    @MethodSource("createLocationChecks")
    void validateLocationTests(
        String locationId,
        String zoneId,
        String stopId,
        String containsId,
        String destinationId,
        String originId,
        NewGTFSErrorType expectedError
    ) {
        List<FareRule> fareRules = new ArrayList<>();
        if (containsId != null || destinationId != null || originId != null) {
            FareRule fareRule = new FareRule();
            fareRule.contains_id = containsId;
            fareRule.destination_id = destinationId;
            fareRule.origin_id = originId;
            fareRules = Lists.newArrayList(fareRule);
        }
        flexValidator.errors.clear();
        flexValidator.validateLocations(
            createLocation(locationId, zoneId),
            Lists.newArrayList(createStop(stopId)),
            fareRules
        );
        checkValidationErrorsMatchExpectedErrors(flexValidator.errors, Lists.newArrayList(expectedError));
    }

    private static Stream<Arguments> createLocationChecks() {
        return Stream.of(
            Arguments.of("1", null, "2", null, null, null, null), // Pass, no id conflicts
            Arguments.of("1", null, "1", null, null, null, NewGTFSErrorType.FLEX_FORBIDDEN_LOCATION_ID),
            Arguments.of("1", "1", "2", null, null, null, null), // Pass, zone id is not required if no fare rules
            Arguments.of("1", "1", "2", "3", "", "", NewGTFSErrorType.FLEX_MISSING_FARE_RULE), // Fail, zone id is required if fare rules are defined
            Arguments.of("1", "1", "2", "", "3", "", NewGTFSErrorType.FLEX_MISSING_FARE_RULE), // Fail, zone id is required if fare rules are defined
            Arguments.of("1", "1", "2", "", "", "3", NewGTFSErrorType.FLEX_MISSING_FARE_RULE), // Fail, zone id is required if fare rules are defined
            Arguments.of("1", "1", "2", "1", "", "", null) // Pass, zone id matches fare rule
        );
    }

    @ParameterizedTest
    @MethodSource("createStopTimeChecksForArrivalDepartureStartEndPickupDropoff")
    void validateStopTimeArrivalDepartureStartEndPickupDropoffTests(
        int arrivalTime,
        int departureTime,
        int startPickupDropOffWindow,
        int endPickupDropOffWindow,
        List<NewGTFSErrorType> expectedErrors
    ) {
        flexValidator.errors.clear();
        StopTime stopTime = new StopTime();
        stopTime.arrival_time = arrivalTime;
        stopTime.departure_time = departureTime;
        stopTime.start_pickup_dropoff_window = startPickupDropOffWindow;
        stopTime.end_pickup_dropoff_window = endPickupDropOffWindow;
        flexValidator.validateStopTimes(
            stopTime,
            null,
            null
        );
        checkValidationErrorsMatchExpectedErrors(flexValidator.errors, expectedErrors);
    }

    private static Stream<Arguments> createStopTimeChecksForArrivalDepartureStartEndPickupDropoff() {
        return Stream.of(
            Arguments.of(1130, INT_MISSING, INT_MISSING, INT_MISSING, null),
            Arguments.of(INT_MISSING, 1330, INT_MISSING, INT_MISSING, null),
            Arguments.of(1130, 1330, INT_MISSING, INT_MISSING, null),
            Arguments.of(INT_MISSING, INT_MISSING, 1100, INT_MISSING, null),
            Arguments.of(INT_MISSING, INT_MISSING, INT_MISSING, 1200, null),
            Arguments.of(INT_MISSING, INT_MISSING, 1100, 1200, null),
            Arguments.of(1130, INT_MISSING, 1100, INT_MISSING,
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_ARRIVAL_TIME, FLEX_FORBIDDEN_START_PICKUP_DROPOFF_WINDOW)),
            Arguments.of(1130, INT_MISSING, INT_MISSING, 1200,
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_ARRIVAL_TIME, FLEX_FORBIDDEN_END_PICKUP_DROPOFF_WINDOW)),
            Arguments.of(INT_MISSING, 1330, 1100, INT_MISSING,
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_DEPARTURE_TIME, FLEX_FORBIDDEN_START_PICKUP_DROPOFF_WINDOW)),
            Arguments.of(INT_MISSING, 1330, INT_MISSING, 1200,
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_DEPARTURE_TIME, FLEX_FORBIDDEN_END_PICKUP_DROPOFF_WINDOW))
        );
    }

    @ParameterizedTest
    @MethodSource("createStopTimeChecksForStartEndPickupDropoff")
    void validateStopTimeStartEndPickupDropoffTests(
        int startPickupDropOffWindow,
        int endPickupDropOffWindow,
        int pickupType,
        int dropOffType,
        String locationId,
        String locationGroupId,
        List<NewGTFSErrorType> expectedErrors
    ) {
        flexValidator.errors.clear();
        StopTime stopTime = new StopTime();
        stopTime.stop_id = "1";
        stopTime.start_pickup_dropoff_window = startPickupDropOffWindow;
        stopTime.end_pickup_dropoff_window = endPickupDropOffWindow;
        stopTime.pickup_type = pickupType;
        stopTime.drop_off_type = dropOffType;
        flexValidator.validateStopTimes(
            stopTime,
            (locationGroupId != null) ? Lists.newArrayList(createLocationGroup(locationGroupId)) : null,
            (locationId != null) ? Lists.newArrayList(createLocation(locationId)) : null
        );
        checkValidationErrorsMatchExpectedErrors(flexValidator.errors, expectedErrors);
    }

    private static Stream<Arguments> createStopTimeChecksForStartEndPickupDropoff() {
        return Stream.of(
            Arguments.of(INT_MISSING, INT_MISSING, INT_MISSING, INT_MISSING, null, null, null), // Pass, no location/location groups
            Arguments.of(1100, 1200, INT_MISSING, INT_MISSING, "1", null, null), // Pass, stop id refers to location and start/end defined.
            Arguments.of(INT_MISSING, 1200, INT_MISSING, INT_MISSING, "1", null,
                Lists.newArrayList(NewGTFSErrorType.FLEX_REQUIRED_START_PICKUP_DROPOFF_WINDOW)),
            Arguments.of(1100, INT_MISSING, INT_MISSING, INT_MISSING, "1", null,
                Lists.newArrayList(NewGTFSErrorType.FLEX_REQUIRED_END_PICKUP_DROPOFF_WINDOW)),
            Arguments.of(1100, 1200, 0, INT_MISSING, "1", null,
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PICKUP_TYPE)),
            // Checks against location group instead of location.
            Arguments.of(1100, 1200, 3, INT_MISSING, null, "1",
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PICKUP_TYPE_FOR_LOCATION_GROUP)),
            Arguments.of(INT_MISSING, 1200, INT_MISSING, INT_MISSING, null, "1",
                Lists.newArrayList(NewGTFSErrorType.FLEX_REQUIRED_START_PICKUP_DROPOFF_WINDOW)),
            Arguments.of(1100, INT_MISSING, INT_MISSING, INT_MISSING, null, "1",
                Lists.newArrayList(NewGTFSErrorType.FLEX_REQUIRED_END_PICKUP_DROPOFF_WINDOW)),
            Arguments.of(1100, 1200, 0, INT_MISSING, null, "1",
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PICKUP_TYPE))
        );
    }

    @ParameterizedTest
    @MethodSource("createStopTimeChecksForMeanAndSafe")
    void validateStopTimeMeanAndSafeTests(
        double meanDurationFactor,
        double meanDurationOffset,
        double safeDurationFactor,
        double safeDurationOffset,
        String locationId,
        String locationGroupId,
        List<NewGTFSErrorType> expectedErrors
    ) {
        flexValidator.errors.clear();
        StopTime stopTime = new StopTime();
        stopTime.stop_id = "1";
        stopTime.mean_duration_factor = meanDurationFactor;
        stopTime.mean_duration_offset = meanDurationOffset;
        stopTime.safe_duration_factor = safeDurationFactor;
        stopTime.safe_duration_offset = safeDurationOffset;
        // Additional parameters to satisfy previous cases which have already been tested.
        stopTime.start_pickup_dropoff_window = 1200;
        stopTime.end_pickup_dropoff_window = 1300;
        stopTime.pickup_type = 1;
        stopTime.drop_off_type = 1;
        flexValidator.validateStopTimes(
            stopTime,
            (locationGroupId != null) ? Lists.newArrayList(createLocationGroup(locationGroupId)) : null,
            (locationId != null) ? Lists.newArrayList(createLocation(locationId)) : null
        );
        checkValidationErrorsMatchExpectedErrors(flexValidator.errors, expectedErrors);
    }

    private static Stream<Arguments> createStopTimeChecksForMeanAndSafe() {
        return Stream.of(
            Arguments.of(1.0, DOUBLE_MISSING, DOUBLE_MISSING, DOUBLE_MISSING, "2", null,
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_MEAN_DURATION_FACTOR)),
            Arguments.of(DOUBLE_MISSING, 1.0, DOUBLE_MISSING, DOUBLE_MISSING, "2", null,
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_MEAN_DURATION_OFFSET)),
            Arguments.of(DOUBLE_MISSING, DOUBLE_MISSING, 1.0, DOUBLE_MISSING, "2", null,
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_SAFE_DURATION_FACTOR)),
            Arguments.of(DOUBLE_MISSING, DOUBLE_MISSING, DOUBLE_MISSING, 1.0, "2", null,
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_SAFE_DURATION_OFFSET)),
            // Checks against location group instead of location.
            Arguments.of(1.0, DOUBLE_MISSING, DOUBLE_MISSING, DOUBLE_MISSING, null, "3",
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_MEAN_DURATION_FACTOR)),
            Arguments.of(DOUBLE_MISSING, 1.0, DOUBLE_MISSING, DOUBLE_MISSING, null, "3",
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_MEAN_DURATION_OFFSET)),
            Arguments.of(DOUBLE_MISSING, DOUBLE_MISSING, 1.0, DOUBLE_MISSING, null, "3",
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_SAFE_DURATION_FACTOR)),
            Arguments.of(DOUBLE_MISSING, DOUBLE_MISSING, DOUBLE_MISSING, 1.0, null, "3",
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_SAFE_DURATION_OFFSET))
        );
    }

    @ParameterizedTest
    @MethodSource("createBookingRuleChecks")
    void validateBookingRuleTests(
        int priorNoticeDurationMin,
        int bookingType,
        int priorNoticeDurationMax,
        int priorNoticeLastDay,
        int priorNoticeStartDay,
        int priorNoticeStartTime,
        String priorNoticeServiceId,
        List<NewGTFSErrorType> expectedErrors
    ) {
        BookingRule bookingRule = new BookingRule();
        bookingRule.prior_notice_duration_min = priorNoticeDurationMin;
        bookingRule.booking_type = bookingType;
        bookingRule.prior_notice_duration_max = priorNoticeDurationMax;
        bookingRule.prior_notice_last_day = priorNoticeLastDay;
        bookingRule.prior_notice_start_day = priorNoticeStartDay;
        bookingRule.prior_notice_start_time = priorNoticeStartTime;
        bookingRule.prior_notice_service_id = priorNoticeServiceId;
        flexValidator.errors.clear();
        flexValidator.validateBookingRules(bookingRule);
        checkValidationErrorsMatchExpectedErrors(flexValidator.errors, expectedErrors);
    }

    private static Stream<Arguments> createBookingRuleChecks() {
        return Stream.of(
            Arguments.of(INT_MISSING, 1, INT_MISSING, INT_MISSING, INT_MISSING, INT_MISSING, null,
                Lists.newArrayList(NewGTFSErrorType.FLEX_REQUIRED_PRIOR_NOTICE_DURATION_MIN)),
            Arguments.of(30, INT_MISSING, INT_MISSING, INT_MISSING, INT_MISSING, INT_MISSING, null,
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_DURATION_MIN)),
            Arguments.of(INT_MISSING, 0, 30, INT_MISSING, INT_MISSING, INT_MISSING, null,
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_DURATION_MAX)),
            Arguments.of(INT_MISSING, 2, 30, INT_MISSING, INT_MISSING, INT_MISSING, null,
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_DURATION_MAX,
                    NewGTFSErrorType.FLEX_REQUIRED_PRIOR_NOTICE_LAST_DAY)),
            Arguments.of(INT_MISSING, 2, INT_MISSING, 1, INT_MISSING, INT_MISSING, null,
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_LAST_DAY)),
            Arguments.of(INT_MISSING, 0, INT_MISSING, INT_MISSING, 1, 1000, null,
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_START_DAY)),
            Arguments.of(30, 1, 30, INT_MISSING, 1, 1030, null,
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_START_DAY_2)),
            Arguments.of(INT_MISSING, INT_MISSING, 30, INT_MISSING, 2, INT_MISSING, null,
                Lists.newArrayList(NewGTFSErrorType.FLEX_REQUIRED_PRIOR_NOTICE_START_TIME)),
            Arguments.of(INT_MISSING, INT_MISSING, INT_MISSING, INT_MISSING, INT_MISSING, 1900, null,
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_START_TIME)),
            Arguments.of(INT_MISSING, 0, INT_MISSING, INT_MISSING, INT_MISSING, INT_MISSING, "1",
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_SERVICE_ID))
        );
    }

    private void checkValidationErrorsMatchExpectedErrors(
        List<NewGTFSError> validationErrors,
        List<NewGTFSErrorType> expectedErrors
    ) {
        if (expectedErrors != null) {
            for (int i=0; i<validationErrors.size(); i++) {
                assertEquals(expectedErrors.get(i), validationErrors.get(i).errorType);
            }
        } else {
            // No errors expected, so the reported errors should be empty.
            assertTrue(flexValidator.errors.isEmpty());
        }
    }

    private static Location createLocation(String locationId) {
        return createLocation(locationId, null);
    }

    private static Location createLocation(String locationId, String zoneId) {
        Location location = new Location();
        location.location_id = locationId;
        location.zone_id = zoneId;
        return location;
    }

    private static LocationGroup createLocationGroup(String locationGroupId) {
        LocationGroup locationGroup = new LocationGroup();
        locationGroup.location_group_id = locationGroupId;
        return locationGroup;
    }

    private static Stop createStop(String stopId) {
        Stop stop = new Stop();
        stop.stop_id = stopId;
        return stop;
    }
}
