package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.model.BookingRule;
import com.conveyal.gtfs.model.FareRule;
import com.conveyal.gtfs.model.Location;
import com.conveyal.gtfs.model.LocationGroup;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import com.google.common.collect.Lists;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.conveyal.gtfs.model.Entity.DOUBLE_MISSING;
import static com.conveyal.gtfs.model.Entity.INT_MISSING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FlexValidatorTest {

    @ParameterizedTest
    @MethodSource("createLocationGroupChecks")
    void validateLocationGroupTests(LocationGroupArguments locationGroupArguments) {
        List<NewGTFSError> errors = FlexValidator.validateLocationGroup(
            (LocationGroup) locationGroupArguments.testObject,
            locationGroupArguments.stops,
            locationGroupArguments.locations
        );
        checkValidationErrorsMatchExpectedErrors(errors, locationGroupArguments.expectedErrors);
    }

    private static Stream<LocationGroupArguments> createLocationGroupChecks() {
        return Stream.of(
            new LocationGroupArguments(
                createLocationGroup("1"),"1", "2",
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_LOCATION_GROUP_ID)
            ),
            new LocationGroupArguments(
                createLocationGroup("1"),"2", "1",
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_LOCATION_GROUP_ID)
            ),
            new LocationGroupArguments(
                createLocationGroup("1"),"2", "3",
                null
            )
        );
    }

    @ParameterizedTest
    @MethodSource("createLocationChecks")
    void validateLocationTests(LocationArguments locationArguments) {
        List<NewGTFSError> errors = FlexValidator.validateLocation(
            (Location) locationArguments.testObject,
            locationArguments.stops,
            locationArguments.fareRules
        );
        checkValidationErrorsMatchExpectedErrors(errors, locationArguments.expectedErrors);
    }

    private static Stream<LocationArguments> createLocationChecks() {
        return Stream.of(
            // Pass, no id conflicts
            new LocationArguments(
                createLocation("1", null),"2", null, null, null,
                null
            ),
            new LocationArguments(
                createLocation("1", null),"1", null, null, null,
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_LOCATION_ID)
            ),
            // Pass, zone id is not required if no fare rules
            new LocationArguments(
                createLocation("1", "1"),"2", null, null, null,
                null
            ),
            new LocationArguments(
                createLocation("1", "1"),"2", "3", "", "",
                Lists.newArrayList(NewGTFSErrorType.FLEX_MISSING_FARE_RULE)
            ),
            new LocationArguments(
                createLocation("1", "1"),"2", "", "3", "",
                Lists.newArrayList(NewGTFSErrorType.FLEX_MISSING_FARE_RULE)
            ),
            new LocationArguments(
                createLocation("1", "1"),"2", "", "", "3",
                Lists.newArrayList(NewGTFSErrorType.FLEX_MISSING_FARE_RULE)
            ),
            // Pass, zone id matches fare rule
            new LocationArguments(
                createLocation("1", "1"),"2", "1", "", "",
                null
            )
        );
    }

    @ParameterizedTest
    @MethodSource("createStopTimeChecksForArrivalDepartureStartEndPickupDropoff")
    void validateStopTimeArrivalDepartureStartEndPickupDropoffTests(BaseArguments baseArguments) {
        List<NewGTFSError> errors = FlexValidator.validateStopTime(
            (StopTime) baseArguments.testObject,
            null,
            null
        );
        checkValidationErrorsMatchExpectedErrors(errors, baseArguments.expectedErrors);
    }

    private static Stream<BaseArguments> createStopTimeChecksForArrivalDepartureStartEndPickupDropoff() {
        return Stream.of(
            new BaseArguments(
                createStopTime(1130, INT_MISSING, INT_MISSING, INT_MISSING),null
            ),
            new BaseArguments(
                createStopTime(INT_MISSING, 1330, INT_MISSING, INT_MISSING),null
            ),
            new BaseArguments(
                createStopTime(1130, 1330, INT_MISSING, INT_MISSING),null
            ),
            new BaseArguments(
                createStopTime(INT_MISSING, INT_MISSING, 1100, INT_MISSING),null
            ),
            new BaseArguments(
                createStopTime(INT_MISSING, INT_MISSING, INT_MISSING, 1200),null
            ),
            new BaseArguments(
                createStopTime(INT_MISSING, INT_MISSING, 1100, 1200),null
            ),
            new BaseArguments(
                createStopTime(1130, INT_MISSING, 1100, INT_MISSING),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_ARRIVAL_TIME, NewGTFSErrorType.FLEX_FORBIDDEN_START_PICKUP_DROPOFF_WINDOW)
            ),
            new BaseArguments(
                createStopTime(1130, INT_MISSING, INT_MISSING, 1200),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_ARRIVAL_TIME, NewGTFSErrorType.FLEX_FORBIDDEN_END_PICKUP_DROPOFF_WINDOW)
            ),
            new BaseArguments(
                createStopTime(INT_MISSING, 1330, 1100, INT_MISSING),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_DEPARTURE_TIME, NewGTFSErrorType.FLEX_FORBIDDEN_START_PICKUP_DROPOFF_WINDOW)
            ),
            new BaseArguments(
                createStopTime(INT_MISSING, 1330, INT_MISSING, 1200),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_DEPARTURE_TIME, NewGTFSErrorType.FLEX_FORBIDDEN_END_PICKUP_DROPOFF_WINDOW)
            )
        );
    }

    @ParameterizedTest
    @MethodSource("createStopTimeChecksForStartEndPickupDropoff")
    void validateStopTimeStartEndPickupDropoffTests(StopTimeArguments stopTimeArguments
    ) {
        List<NewGTFSError> errors = FlexValidator.validateStopTime(
            (StopTime) stopTimeArguments.testObject,
            stopTimeArguments.locationGroups,
            stopTimeArguments.locations
        );
        checkValidationErrorsMatchExpectedErrors(errors, stopTimeArguments.expectedErrors);
    }

    private static Stream<StopTimeArguments> createStopTimeChecksForStartEndPickupDropoff() {
        return Stream.of(
            new StopTimeArguments(
                createStopTime("1", INT_MISSING, INT_MISSING, INT_MISSING, INT_MISSING),
                null, null,
                null
            ),
            new StopTimeArguments(
                createStopTime("1", 1100, 1200, INT_MISSING, INT_MISSING),
                null, null,
                null
            ),
            new StopTimeArguments(
                createStopTime("1", INT_MISSING, 1200, INT_MISSING, INT_MISSING),
                "1", null,
                Lists.newArrayList(NewGTFSErrorType.FLEX_REQUIRED_START_PICKUP_DROPOFF_WINDOW)
            ),
            new StopTimeArguments(
                createStopTime("1", 1100, INT_MISSING, INT_MISSING, INT_MISSING),
                "1", null,
                Lists.newArrayList(NewGTFSErrorType.FLEX_REQUIRED_END_PICKUP_DROPOFF_WINDOW)
            ),
            new StopTimeArguments(
                createStopTime("1", 1100, 1200, 0, INT_MISSING),
                "1", null,
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PICKUP_TYPE)
            ),
            new StopTimeArguments(
                createStopTime("1", 1100, 1200, 3, INT_MISSING),
                null, "1",
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PICKUP_TYPE_FOR_LOCATION_GROUP)
            ),
            new StopTimeArguments(
                createStopTime("1", INT_MISSING, 1200, INT_MISSING, INT_MISSING),
                null, "1",
                Lists.newArrayList(NewGTFSErrorType.FLEX_REQUIRED_START_PICKUP_DROPOFF_WINDOW)
            ),
            new StopTimeArguments(
                createStopTime("1", 1100, INT_MISSING, INT_MISSING, INT_MISSING),
                null, "1",
                Lists.newArrayList(NewGTFSErrorType.FLEX_REQUIRED_END_PICKUP_DROPOFF_WINDOW)
            ),
            new StopTimeArguments(
                createStopTime("1", 1100, 1200, 0, INT_MISSING),
                null, "1",
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PICKUP_TYPE)
            ),
            new StopTimeArguments(
                createStopTime("1", 1100, 1200, INT_MISSING, 0),
                null, "1",
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_DROP_OFF_TYPE)
            )
        );
    }

    @ParameterizedTest
    @MethodSource("createStopTimeChecksForMeanAndSafe")
    void validateStopTimeMeanAndSafeTests(StopTimeArguments stopTimeArguments) {
        List<NewGTFSError> errors = FlexValidator.validateStopTime(
            (StopTime) stopTimeArguments.testObject,
            stopTimeArguments.locationGroups,
            stopTimeArguments.locations
        );
        checkValidationErrorsMatchExpectedErrors(errors, stopTimeArguments.expectedErrors);
    }

    private static Stream<StopTimeArguments> createStopTimeChecksForMeanAndSafe() {
        return Stream.of(
            new StopTimeArguments(
                createStopTime(1.0, DOUBLE_MISSING, DOUBLE_MISSING, DOUBLE_MISSING),
                "2", null,
                Lists.newArrayList(
                    NewGTFSErrorType.FLEX_FORBIDDEN_MEAN_DURATION_FACTOR,
                    NewGTFSErrorType.FLEX_SAFE_FACTORS_EXCEEDED
                )
            ),
            new StopTimeArguments(
                createStopTime(DOUBLE_MISSING, 1.0, DOUBLE_MISSING, DOUBLE_MISSING),
                "2", null,
                Lists.newArrayList(
                    NewGTFSErrorType.FLEX_FORBIDDEN_MEAN_DURATION_OFFSET,
                    NewGTFSErrorType.FLEX_SAFE_FACTORS_EXCEEDED
                )
            ),
            new StopTimeArguments(
                createStopTime(DOUBLE_MISSING, DOUBLE_MISSING, 1.0, DOUBLE_MISSING),
                "2", null,
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_SAFE_DURATION_FACTOR)
            ),
            new StopTimeArguments(
                createStopTime(DOUBLE_MISSING, DOUBLE_MISSING, DOUBLE_MISSING, 1.0),
                "2", null,
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_SAFE_DURATION_OFFSET)
            ),
            // Checks against location group instead of location.
            new StopTimeArguments(
                createStopTime(1.0, DOUBLE_MISSING, DOUBLE_MISSING, DOUBLE_MISSING),
                null, "2",
                Lists.newArrayList(
                    NewGTFSErrorType.FLEX_FORBIDDEN_MEAN_DURATION_FACTOR,
                    NewGTFSErrorType.FLEX_SAFE_FACTORS_EXCEEDED
                )
            ),
            new StopTimeArguments(
                createStopTime(DOUBLE_MISSING, 1.0, DOUBLE_MISSING, DOUBLE_MISSING),
                null, "2",
                Lists.newArrayList(
                    NewGTFSErrorType.FLEX_FORBIDDEN_MEAN_DURATION_OFFSET,
                    NewGTFSErrorType.FLEX_SAFE_FACTORS_EXCEEDED
                )
            ),
            new StopTimeArguments(
                createStopTime(DOUBLE_MISSING, DOUBLE_MISSING, 1.0, DOUBLE_MISSING),
                null, "2",
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_SAFE_DURATION_FACTOR)
            ),
            new StopTimeArguments(
                createStopTime(DOUBLE_MISSING, DOUBLE_MISSING, DOUBLE_MISSING, 1.0),
                null, "2",
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_SAFE_DURATION_OFFSET)
            ),
            new StopTimeArguments(
                createStopTime(30.0, 1.0, 20.0, 1.0),
                null, "1",
                Lists.newArrayList(NewGTFSErrorType.FLEX_SAFE_FACTORS_EXCEEDED)
            )
        );
    }

    @ParameterizedTest
    @MethodSource("createBookingRuleChecks")
    void validateBookingRuleTests(BaseArguments baseArguments) {
        List<NewGTFSError> errors = FlexValidator.validateBookingRule((BookingRule) baseArguments.testObject);
        checkValidationErrorsMatchExpectedErrors(errors, baseArguments.expectedErrors);
    }

    private static Stream<BaseArguments> createBookingRuleChecks() {
        return Stream.of(
            new BaseArguments(
                createBookingRule(INT_MISSING, 1, INT_MISSING, INT_MISSING, INT_MISSING, INT_MISSING, null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_REQUIRED_PRIOR_NOTICE_DURATION_MIN)
            ),
            new BaseArguments(
                createBookingRule(30, INT_MISSING, INT_MISSING, INT_MISSING, INT_MISSING, INT_MISSING, null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_DURATION_MIN)
            ),
            new BaseArguments(
                createBookingRule(INT_MISSING, 0, 30, INT_MISSING, INT_MISSING, INT_MISSING, null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_DURATION_MAX)
            ),
            new BaseArguments(
                createBookingRule(INT_MISSING, 2, 30, INT_MISSING, INT_MISSING, INT_MISSING, null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_DURATION_MAX, NewGTFSErrorType.FLEX_REQUIRED_PRIOR_NOTICE_LAST_DAY)
            ),
            new BaseArguments(
                createBookingRule(INT_MISSING, 2, INT_MISSING, 1, INT_MISSING, INT_MISSING, null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_LAST_DAY)
            ),
            new BaseArguments(
                createBookingRule(INT_MISSING, 0, INT_MISSING, INT_MISSING, 1, 1000, null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_START_DAY_FOR_BOOKING_TYPE)
            ),
            new BaseArguments(
                createBookingRule(30, 1, 30, INT_MISSING, 1, 1030, null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_START_DAY)
            ),
            new BaseArguments(
                createBookingRule(INT_MISSING, INT_MISSING, 30, INT_MISSING, 2, INT_MISSING, null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_REQUIRED_PRIOR_NOTICE_START_TIME)
            ),
            new BaseArguments(
                createBookingRule(INT_MISSING, INT_MISSING, INT_MISSING, INT_MISSING, INT_MISSING, 1900, null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_START_TIME)
            ),
            new BaseArguments(
                createBookingRule(INT_MISSING, 0, INT_MISSING, INT_MISSING, INT_MISSING, INT_MISSING, "1"),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_SERVICE_ID)
            )
        );
    }

    @ParameterizedTest
    @MethodSource("createTripChecks")
    void validateTripTests(TripArguments tripArguments) {
        List<NewGTFSError> errors = FlexValidator.validateTrip(
            (Trip) tripArguments.testObject,
            tripArguments.stopTimes,
            tripArguments.locationGroups,
            tripArguments.locations
        );
        checkValidationErrorsMatchExpectedErrors(errors, tripArguments.expectedErrors);
    }

    private static Stream<TripArguments> createTripChecks() {
        return Stream.of(
            new TripArguments(
                createTrip(), "1", "0","0",
                null
            ),
            new TripArguments(
                createTrip(), "1", "1","0",
                Lists.newArrayList(NewGTFSErrorType.TRIP_SPEED_NOT_VALIDATED)
            ),
            new TripArguments(
                createTrip(), "1", "0","1",
                Lists.newArrayList(NewGTFSErrorType.TRIP_SPEED_NOT_VALIDATED)
            )
        );
    }

    private static class BaseArguments {
        public Object testObject;
        public List<NewGTFSErrorType> expectedErrors;

        private BaseArguments(Object testData, List<NewGTFSErrorType> expectedErrors) {
            this.testObject = testData;
            this.expectedErrors = expectedErrors;
        }
    }

    private static class StopTimeArguments extends BaseArguments {
        public final List<LocationGroup> locationGroups;
        public final List<Location> locations;

        private StopTimeArguments(
            Object stopTime,
            String locationId,
            String locationGroupId,
            List<NewGTFSErrorType> expectedErrors
        ) {
            super(stopTime, expectedErrors);
            this.locationGroups = (locationGroupId != null) ? Lists.newArrayList(createLocationGroup(locationGroupId)) : null;
            this.locations = (locationId != null) ? Lists.newArrayList(createLocation(locationId)) : null;
       }
    }

    private static class LocationArguments extends BaseArguments {
        public final List<Stop> stops;
        public List<FareRule> fareRules = new ArrayList<>();

        private LocationArguments(
            Object location,
            String stopId,
            String containsId,
            String destinationId,
            String originId,
            List<NewGTFSErrorType> expectedErrors
        ) {
            super(location, expectedErrors);
            this.stops = Lists.newArrayList(createStop(stopId));
            if (containsId != null || destinationId != null || originId != null) {
                FareRule fareRule = new FareRule();
                fareRule.contains_id = containsId;
                fareRule.destination_id = destinationId;
                fareRule.origin_id = originId;
                fareRules = Lists.newArrayList(fareRule);
            }

        }
    }

    private static class LocationGroupArguments extends BaseArguments {
        public final List<Stop> stops;
        public List<Location> locations;

        private LocationGroupArguments(
            Object locationGroup,
            String locationId,
            String stopId,
            List<NewGTFSErrorType> expectedErrors
        ) {
            super(locationGroup, expectedErrors);
            this.stops = Lists.newArrayList(createStop(stopId));
            this.locations = Lists.newArrayList(createLocation(locationId));
        }
    }

    private static class TripArguments extends BaseArguments {
        public final List<StopTime> stopTimes;
        public final List<LocationGroup> locationGroups;
        public final List<Location> locations;

        private TripArguments(
            Object trip,
            String stopId,
            String locationId,
            String locationGroupId,
            List<NewGTFSErrorType> expectedErrors
        ) {
            super(trip, expectedErrors);
            this.stopTimes = (stopId != null) ? Lists.newArrayList(createStopTime(stopId, ((Trip)trip).trip_id)) : null;
            this.locationGroups = (locationGroupId != null) ? Lists.newArrayList(createLocationGroup(locationGroupId)) : null;
            this.locations = (locationId != null) ? Lists.newArrayList(createLocation(locationId)) : null;
        }
    }


    /**
     * Check that the errors produced by the flex validator match the expected errors. If no errors are expected, check
     * that no errors were produced. If errors are expected loop over the validation errors so as not to hide any
     * unexpected errors.
     */
    private void checkValidationErrorsMatchExpectedErrors(
        List<NewGTFSError> validationErrors,
        List<NewGTFSErrorType> expectedErrors
    ) {
        if (expectedErrors != null) {
            for (int i = 0; i < validationErrors.size(); i++) {
                assertEquals(expectedErrors.get(i), validationErrors.get(i).errorType);
            }
        } else {
            // No errors expected, so the reported errors should be empty.
            assertTrue(validationErrors.isEmpty());
        }
    }

    private static BookingRule createBookingRule(
        int priorNoticeDurationMin,
        int bookingType,
        int priorNoticeDurationMax,
        int priorNoticeLastDay,
        int priorNoticeStartDay,
        int priorNoticeStartTime,
        String priorNoticeServiceId
    ) {
        BookingRule bookingRule = new BookingRule();
        bookingRule.prior_notice_duration_min = priorNoticeDurationMin;
        bookingRule.booking_type = bookingType;
        bookingRule.prior_notice_duration_max = priorNoticeDurationMax;
        bookingRule.prior_notice_last_day = priorNoticeLastDay;
        bookingRule.prior_notice_start_day = priorNoticeStartDay;
        bookingRule.prior_notice_start_time = priorNoticeStartTime;
        bookingRule.prior_notice_service_id = priorNoticeServiceId;
        return bookingRule;
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

    private static StopTime createStopTime(
        double meanDurationFactor,
        double meanDurationOffset,
        double safeDurationFactor,
        double safeDurationOffset
    ) {
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
        return stopTime;
    }

    private static StopTime createStopTime(
        String stopId,
        int startPickupDropOffWindow,
        int endPickupDropOffWindow,
        int pickupType,
        int dropOffType
    ) {
        StopTime stopTime = new StopTime();
        stopTime.stop_id = stopId;
        stopTime.start_pickup_dropoff_window = startPickupDropOffWindow;
        stopTime.end_pickup_dropoff_window = endPickupDropOffWindow;
        stopTime.pickup_type = pickupType;
        stopTime.drop_off_type = dropOffType;
        return stopTime;
    }

    private static StopTime createStopTime(
        int arrivalTime,
        int departureTime,
        int startPickupDropOffWindow,
        int endPickupDropOffWindow
    ) {
        StopTime stopTime = new StopTime();
        stopTime.arrival_time = arrivalTime;
        stopTime.departure_time = departureTime;
        stopTime.start_pickup_dropoff_window = startPickupDropOffWindow;
        stopTime.end_pickup_dropoff_window = endPickupDropOffWindow;
        return stopTime;
    }

    private static StopTime createStopTime(String stopId, String tripId) {
        StopTime stopTime = new StopTime();
        stopTime.stop_id = stopId;
        stopTime.trip_id = tripId;
        return stopTime;
    }

    private static Trip createTrip() {
        Trip trip = new Trip();
        trip.trip_id = "12345";
        return trip;
    }
}
