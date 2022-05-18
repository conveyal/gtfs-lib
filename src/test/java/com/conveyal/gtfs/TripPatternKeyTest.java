package com.conveyal.gtfs;

import com.conveyal.gtfs.model.StopTime;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static com.conveyal.gtfs.model.Entity.INT_MISSING;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TripPatternKeyTest {
    /**
     * A StopTime instance with pickup/drop-off field set to the default value (0 - regular) and another instance
     * with the same field omitted should result to two TripPatternKey objects that are deemed equal.
     */
    @ParameterizedTest
    @MethodSource("createStopTimesForPickupAndDropOffChecks")
    void shouldConsiderSetAndUnsetPickupDropOffs(StopTime testStopTime, String omittedField) {
        // Note: pickup_type and drop_off_type are initialized to zero by default.
        StopTime commonStopTime = new StopTime();
        commonStopTime.stop_id = "stop1";

        StopTime referenceStopTime = new StopTime();
        referenceStopTime.stop_id = "stop2";
        testStopTime.stop_id = "stop2";

        String routeId = "test-route";

        TripPatternKey referenceKey = new TripPatternKey(routeId);
        referenceKey.addStopTime(commonStopTime);
        referenceKey.addStopTime(referenceStopTime);

        TripPatternKey otherKey = new TripPatternKey(routeId);
        otherKey.addStopTime(commonStopTime);
        otherKey.addStopTime(testStopTime);

        assertEquals(referenceKey, otherKey, "TripPatternKey did not correctly interpret missing value for" + omittedField);
    }

    private static Stream<Arguments> createStopTimesForPickupAndDropOffChecks() {
        StopTime st1 = new StopTime();
        st1.pickup_type = INT_MISSING;

        StopTime st2 = new StopTime();
        st2.drop_off_type = INT_MISSING;

        return Stream.of(
            Arguments.of(st1, "pickup_type"),
            Arguments.of(st2, "dropoff_type")
        );
    }
}
