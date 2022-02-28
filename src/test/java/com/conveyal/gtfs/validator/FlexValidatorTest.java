package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.model.FareRule;
import com.conveyal.gtfs.model.Location;
import com.conveyal.gtfs.model.LocationGroup;
import com.conveyal.gtfs.model.Stop;
import com.google.common.collect.Lists;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

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
        if (expectedError != null) {
            assertEquals(flexValidator.errors.get(0).errorType, expectedError);
        } else {
            // No errors reported (or expected) so this should be empty.
            assertTrue(flexValidator.errors.isEmpty());
        }
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
            fareRules = Lists.newArrayList(createFareRule(containsId, destinationId, originId));
        }
        flexValidator.errors.clear();
        flexValidator.validateLocations(
            createLocation(locationId, zoneId),
            Lists.newArrayList(createStop(stopId)),
            fareRules
        );
        if (expectedError != null) {
            assertEquals(flexValidator.errors.get(0).errorType, expectedError);
        } else {
            // No errors reported (or expected) so this should be empty.
            assertTrue(flexValidator.errors.isEmpty());
        }
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

    private static FareRule createFareRule(String containsId, String destinationId, String originId) {
        FareRule fareRule = new FareRule();
        fareRule.contains_id = containsId;
        fareRule.destination_id = destinationId;
        fareRule.origin_id = originId;
        return fareRule;
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

    private static Stop createStop(String stopId) {
        Stop stop = new Stop();
        stop.stop_id = stopId;
        return stop;
    }

}
