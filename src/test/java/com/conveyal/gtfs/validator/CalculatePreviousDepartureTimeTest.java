package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.TripPatternKey;
import com.conveyal.gtfs.model.StopTime;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CalculatePreviousDepartureTimeTest {
    private final PatternFinderValidator patternFinderValidator = new PatternFinderValidator(null, null);

    @ParameterizedTest
    @MethodSource("createTrips")
    void calculatePreviousDepartureTimeForTrip(TripPatternKey key, List<Departure> departures) {
        int lastValidDepartureTime = (departures.get(0).currentIsFlexStop)
            ? key.end_pickup_dropoff_window.get(0)
            : key.departureTimes.get(0);
        for (int stopSequence = 0; stopSequence < key.stops.size(); stopSequence++) {
            lastValidDepartureTime = patternFinderValidator.calculatePreviousDepartureTime(
                departures.get(stopSequence).prevIsFlexStop,
                departures.get(stopSequence).currentIsFlexStop,
                lastValidDepartureTime,
                key,
                stopSequence
            );
            assertEquals(departures.get(stopSequence).expectedDepartureTime, lastValidDepartureTime);
        }
    }

    /**
     * Produce the required trips for testing.
     */
    private static Stream<Arguments> createTrips() {
        // Confirm that a trip that consists of just points (e.g. bus stops) will produce the correct departure times.
        TripPatternKey pointToPointKey = new TripPatternKey("test-route");
        pointToPointKey.addStopTime(createStopTime(2, 0));
        pointToPointKey.addStopTime(createStopTime(3, 0));
        pointToPointKey.addStopTime(createStopTime(4, 0));
        pointToPointKey.addStopTime(createStopTime(5, 0));
        pointToPointKey.addStopTime(createStopTime(6, 0));

        List<Departure> pointToPointDepartures = new ArrayList<>();
        pointToPointDepartures.add(new Departure(false, false, 2, pointToPointKey, 0));
        pointToPointDepartures.add(new Departure(false, false, 2, pointToPointKey, 1));
        pointToPointDepartures.add(new Departure(false, false, 3, pointToPointKey, 2));
        pointToPointDepartures.add(new Departure(false, false, 4, pointToPointKey, 3));
        pointToPointDepartures.add(new Departure(false, false, 5, pointToPointKey, 4));

        // Confirm that a trip that consists of just flex stops will produce the correct departure times.
        TripPatternKey flexKey = new TripPatternKey("test-route");
        flexKey.addStopTime(createStopTime(0, 600));
        flexKey.addStopTime(createStopTime(0, 720));

        List<Departure> flexDepartures = new ArrayList<>();
        flexDepartures.add(new Departure(true, true, 600, flexKey, 0));
        flexDepartures.add(new Departure(true, true, 0, flexKey, 1));

        // Confirm that a combination of point and flex stops will produce the correct departure times.
        TripPatternKey flexAndPointKey = new TripPatternKey("test-route");
        flexAndPointKey.addStopTime(createStopTime(2, 0));
        flexAndPointKey.addStopTime(createStopTime(0, 600));
        flexAndPointKey.addStopTime(createStopTime(0, 720));
        flexAndPointKey.addStopTime(createStopTime(722, 0));
        flexAndPointKey.addStopTime(createStopTime(0, 900));
        flexAndPointKey.addStopTime(createStopTime(903, 0));
        flexAndPointKey.addStopTime(createStopTime(904, 0));

        List<Departure> flexAndPointDepartures = new ArrayList<>();
        flexAndPointDepartures.add(new Departure(false, false, 2, flexAndPointKey, 0));
        flexAndPointDepartures.add(new Departure(false, true, 2, flexAndPointKey, 0));
        flexAndPointDepartures.add(new Departure(true, true, 0, flexAndPointKey, 1));
        flexAndPointDepartures.add(new Departure(true, false, 720, flexAndPointKey, 1));
        flexAndPointDepartures.add(new Departure(false, true, 722, flexAndPointKey, 1));
        flexAndPointDepartures.add(new Departure(true, false, 900, flexAndPointKey, 1));
        flexAndPointDepartures.add(new Departure(false, false, 903, flexAndPointKey, 1));

        // Confirm that a trip that consists of invalid departure times will still produce the correct departure times.
        TripPatternKey pointToPointWithInvalidDeparturesKey = new TripPatternKey("test-route");
        pointToPointWithInvalidDeparturesKey.addStopTime(createStopTime(2, 0));
        pointToPointWithInvalidDeparturesKey.addStopTime(createStopTime(3, 0));
        pointToPointWithInvalidDeparturesKey.addStopTime(createStopTime(0, 0));
        pointToPointWithInvalidDeparturesKey.addStopTime(createStopTime(1, 0));
        pointToPointWithInvalidDeparturesKey.addStopTime(createStopTime(6, 0));

        List<Departure> pointToPointWithInvalidDepartures = new ArrayList<>();
        pointToPointWithInvalidDepartures.add(new Departure(false, false, 2, pointToPointWithInvalidDeparturesKey, 0));
        pointToPointWithInvalidDepartures.add(new Departure(false, false, 2, pointToPointWithInvalidDeparturesKey, 1));
        pointToPointWithInvalidDepartures.add(new Departure(false, false, 3, pointToPointWithInvalidDeparturesKey, 2));
        pointToPointWithInvalidDepartures.add(new Departure(false, false, 3, pointToPointWithInvalidDeparturesKey, 3));
        pointToPointWithInvalidDepartures.add(new Departure(false, false, 3, pointToPointWithInvalidDeparturesKey, 4));

        return Stream.of(
            Arguments.of(pointToPointKey, pointToPointDepartures),
            Arguments.of(flexKey, flexDepartures),
            Arguments.of(flexAndPointKey, flexAndPointDepartures),
            Arguments.of(pointToPointWithInvalidDeparturesKey, pointToPointWithInvalidDepartures)
        );
    }

    private static StopTime createStopTime(int departureTime, int endPickupDropOffWindow) {
        StopTime stopTime = new StopTime();
        stopTime.departure_time = departureTime;
        stopTime.end_pickup_dropoff_window = endPickupDropOffWindow;
        return stopTime;
    }

    private static class Departure {
        boolean prevIsFlexStop;
        boolean currentIsFlexStop;
        int expectedDepartureTime;
        TripPatternKey key;
        int stopSequence;

        public Departure(
            boolean prevIsFlexStop,
            boolean currentIsFlexStop,
            int expectedDepartureTime,
            TripPatternKey key,
            int stopSequence
        ) {
            this.prevIsFlexStop = prevIsFlexStop;
            this.currentIsFlexStop = currentIsFlexStop;
            this.expectedDepartureTime = expectedDepartureTime;
            this.key = key;
            this.stopSequence = stopSequence;
        }
    }
}
