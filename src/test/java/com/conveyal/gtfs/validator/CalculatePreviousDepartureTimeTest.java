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
        pointToPointKey.addStopTime(createStopTime("1",2, 0));
        pointToPointKey.addStopTime(createStopTime("2", 3, 0));
        pointToPointKey.addStopTime(createStopTime("3", 4, 0));
        pointToPointKey.addStopTime(createStopTime("4", 5, 0));
        pointToPointKey.addStopTime(createStopTime("5", 6, 0));

        List<Integer> pointToPointExpectedDepartures = Lists.newArrayList(2, 2, 3, 4, 5);

        // Confirm that a trip that consists of just flex stops will produce the correct departure times.
        TripPatternKey flexKey = new TripPatternKey("test-route");
        flexKey.addStopTime(createStopTime("1", 0, 600));
        flexKey.addStopTime(createStopTime("2", 0, 720));

        List<Integer> flexExpectedDepartures = Lists.newArrayList(600, 0);
        Map<String, Location> flexLocationById = new HashMap<>();
        flexLocationById.put("1", null);
        flexLocationById.put("2", null);

        // Confirm that a combination of point and flex stops will produce the correct departure times.
        TripPatternKey flexAndPointKey = new TripPatternKey("test-route");
        flexAndPointKey.addStopTime(createStopTime("1", 2, 0));
        flexAndPointKey.addStopTime(createStopTime("2", 0, 600));
        flexAndPointKey.addStopTime(createStopTime("3",0, 720));
        flexAndPointKey.addStopTime(createStopTime("4", 722, 0));
        flexAndPointKey.addStopTime(createStopTime("5", 0, 900));
        flexAndPointKey.addStopTime(createStopTime("6", 903, 0));
        flexAndPointKey.addStopTime(createStopTime("7", 904, 0));

        List<Integer> flexAndPointExpectedDepartures = Lists.newArrayList(2, 2, 0, 720, 722, 900, 903);
        Map<String, Location> flexAndPointLocationById = new HashMap<>();
        flexAndPointLocationById.put("2", null);
        flexAndPointLocationById.put("3", null);
        flexAndPointLocationById.put("5", null);

        // Confirm that a trip that consists of invalid departure times will still produce the correct departure times.
        TripPatternKey pointToPointWithInvalidDeparturesKey = new TripPatternKey("test-route");
        pointToPointWithInvalidDeparturesKey.addStopTime(createStopTime("1", 2, 0));
        pointToPointWithInvalidDeparturesKey.addStopTime(createStopTime("2", 3, 0));
        pointToPointWithInvalidDeparturesKey.addStopTime(createStopTime("3", 0, 0));
        pointToPointWithInvalidDeparturesKey.addStopTime(createStopTime("4", 1, 0));
        pointToPointWithInvalidDeparturesKey.addStopTime(createStopTime("5", 6, 0));

        List<Integer> pointToPointWithInvalidDepartures = Lists.newArrayList(2, 2, 3, 3, 3);

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
        stopTime.end_pickup_dropoff_window = endPickupDropOffWindow;
        return stopTime;
    }
}
