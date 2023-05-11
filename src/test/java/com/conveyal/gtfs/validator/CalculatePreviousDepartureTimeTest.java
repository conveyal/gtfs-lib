package com.conveyal.gtfs.validator;

import com.beust.jcommander.internal.Lists;
import com.conveyal.gtfs.TripPatternKey;
import com.conveyal.gtfs.model.Location;
import com.conveyal.gtfs.model.StopTime;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CalculatePreviousDepartureTimeTest {
    private final PatternFinderValidator patternFinderValidator = new PatternFinderValidator(null, null);

    @ParameterizedTest
    @MethodSource("createTrips")
    void calculatePreviousDepartureTimeForTrip(
        TripPatternKey key,
        List<Integer> expectedDepartureTimes,
        Map<String, Location> locationById
    ) {
        List<Integer> actualDepartureTimes = patternFinderValidator.calculatePreviousDepartureTimes(
            key,
            locationById,
            new HashMap<>()
        );
        assertEquals(expectedDepartureTimes, actualDepartureTimes);
    }

    /**
     * Produce the required trips for testing.
     */
    private static Stream<Arguments> createTrips() {
        // Confirm that a trip that consists of just points (e.g. bus stops) will produce the correct departure times.
        TripPatternKey pointToPointKey = new TripPatternKey("test-route");
        pointToPointKey.addStopTime(createStopTime("stop-id-1", 2, 0));
        pointToPointKey.addStopTime(createStopTime("stop-id-2", 3, 0));
        pointToPointKey.addStopTime(createStopTime("stop-id-3", 4, 0));
        pointToPointKey.addStopTime(createStopTime("stop-id-4", 5, 0));
        pointToPointKey.addStopTime(createStopTime("stop-id-5", 6, 0));

        List<Integer> pointToPointExpectedDepartures = Lists.newArrayList(0, 2, 3, 4, 5);

        // Confirm that a trip that consists of just flex stops will produce the correct departure times.
        TripPatternKey flexKey = new TripPatternKey("test-route");
        flexKey.addStopTime(createStopTime("stop-id-1", 0, 600));
        flexKey.addStopTime(createStopTime("stop-id-2", 0, 720));

        List<Integer> flexExpectedDepartures = Lists.newArrayList(0, 0);
        Map<String, Location> flexLocationById = new HashMap<>();
        flexLocationById.put("stop-id-1", null);
        flexLocationById.put("stop-id-2", null);

        // Confirm that a combination of point and flex stops will produce the correct departure times.
        TripPatternKey flexAndPointKey = new TripPatternKey("test-route");
        flexAndPointKey.addStopTime(createStopTime("stop-id-1", 2, 0));
        flexAndPointKey.addStopTime(createStopTime("stop-id-2", 0, 600));
        flexAndPointKey.addStopTime(createStopTime("stop-id-3",0, 720));
        flexAndPointKey.addStopTime(createStopTime("stop-id-4", 722, 0));
        flexAndPointKey.addStopTime(createStopTime("stop-id-5", 0, 900));
        flexAndPointKey.addStopTime(createStopTime("stop-id-6", 903, 0));
        flexAndPointKey.addStopTime(createStopTime("stop-id-7", 904, 0));

        List<Integer> flexAndPointExpectedDepartures = Lists.newArrayList(0, 2, 0, 720, 722, 900, 903);
        Map<String, Location> flexAndPointLocationById = new HashMap<>();
        flexAndPointLocationById.put("stop-id-2", null);
        flexAndPointLocationById.put("stop-id-3", null);
        flexAndPointLocationById.put("stop-id-5", null);

        // Confirm that a trip that consists of invalid departure times will still produce the correct departure times.
        TripPatternKey pointToPointWithInvalidDeparturesKey = new TripPatternKey("test-route");
        pointToPointWithInvalidDeparturesKey.addStopTime(createStopTime("stop-id-1", 2, 0));
        pointToPointWithInvalidDeparturesKey.addStopTime(createStopTime("stop-id-2", 3, 0));
        pointToPointWithInvalidDeparturesKey.addStopTime(createStopTime("stop-id-3", 0, 0));
        pointToPointWithInvalidDeparturesKey.addStopTime(createStopTime("stop-id-4", 1, 0));
        pointToPointWithInvalidDeparturesKey.addStopTime(createStopTime("stop-id-5", 6, 0));

        List<Integer> pointToPointWithInvalidDepartures = Lists.newArrayList(0, 2, 3, 3, 3);

        return Stream.of(
            Arguments.of(pointToPointKey, pointToPointExpectedDepartures, new HashMap<>()),
            Arguments.of(flexKey, flexExpectedDepartures, flexLocationById),
            Arguments.of(flexAndPointKey, flexAndPointExpectedDepartures, flexAndPointLocationById),
            Arguments.of(pointToPointWithInvalidDeparturesKey, pointToPointWithInvalidDepartures, new HashMap<>())
        );
    }

    private static StopTime createStopTime(String stopId, int departureTime, int endPickupDropOffWindow) {
        StopTime stopTime = new StopTime();
        stopTime.stop_id = stopId;
        stopTime.departure_time = departureTime;
        stopTime.end_pickup_drop_off_window = endPickupDropOffWindow;
        return stopTime;
    }
}
