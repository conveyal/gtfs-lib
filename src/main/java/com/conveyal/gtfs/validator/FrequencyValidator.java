package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.Frequency;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.List;

public class FrequencyValidator extends FeedValidator {

    /**
     * Validate frequency entries to ensure that there are no overlapping frequency periods defined for a single trip.
     * @param feed
     * @param errorStorage
     */
    public FrequencyValidator(Feed feed, SQLErrorStorage errorStorage) {
        super(feed, errorStorage);
    }

    private ListMultimap<String, Frequency> frequenciesById = ArrayListMultimap.create();

    @Override
    public void validate() {
        // First, collect all frequencies for each trip ID.
        for (Frequency frequency: feed.frequencies) frequenciesById.put(frequency.trip_id, frequency);
        // Next iterate over each set of trip-specific frequency periods.
        for (String tripId : frequenciesById.keySet()) {
            List<Frequency> frequencies = frequenciesById.get(tripId);
            if (frequencies.size() <= 1) {
                // If there are not more than one frequencies defined for the trip, there can be no risk of overlapping
                // frequency intervals.
                return;
            }
            // Iterate over each frequency and check its period against the others for overlap.
            for (int i = 0; i < frequencies.size() - 1; i++) {
                Frequency a = frequencies.get(i);
                // Iterate over the other frequencies starting with i + 1 to avoid checking against self and re-checking
                // previous pairs.
                for (int j = i + 1; j < frequencies.size(); j++) {
                    Frequency b = frequencies.get(j);
                    if (
                        // -- diagrams courtesy of esiroky --
                        // A wraps B.
                        // A: |---------|
                        // B: ___|--|____
                        b.start_time >= a.start_time && b.end_time <= a.end_time ||
                        // B wraps A.
                        // A: ___|--|____
                        // B: |---------|
                        a.start_time >= b.start_time && a.end_time <= b.end_time ||
                        // A starts during B, but ends after B ends.
                        // A: ____|-----|
                        // B: _|----|____
                        a.start_time >= b.start_time && a.start_time < b.end_time ||
                        // B starts during A, but ends after A ends
                        // A: _|----|____
                        // B: ____|-----|
                        a.end_time > b.start_time && a.end_time <= b.end_time
                    ) {
                        registerError(a, NewGTFSErrorType.FREQUENCY_PERIOD_OVERLAP);
                    }
                }
            }
        }
    }
}
