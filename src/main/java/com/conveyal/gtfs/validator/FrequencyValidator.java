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
            if (frequencies.size() < 1) {
                // If there are not more than one frequencies defined for the trip, there can be no risk of overlapping
                // frequency intervals.
                return;
            }
            // Iterate over each frequency and check its period against the others for overlap.
            for (int i = 0; i < frequencies.size(); i++) {
                Frequency frequency = frequencies.get(i);
                // Iterate over the other frequencies.
                for (int j = 0; j < frequencies.size(); j++) {
                    // No need to check against self.
                    if (i == j) continue;
                    Frequency prevFrequency = frequencies.get(j);
                    if (
                        // Other frequency wraps current frequency.
                        frequency.end_time <= prevFrequency.end_time && frequency.start_time >= prevFrequency.start_time ||
                            // Frequency starts during other frequency.
                            frequency.start_time >= prevFrequency.start_time && frequency.start_time < prevFrequency.end_time ||
                            // Frequency ends during other frequency.
                            frequency.end_time > prevFrequency.start_time && frequency.end_time <= prevFrequency.end_time
                    ) {
                        registerError(frequency, NewGTFSErrorType.FREQUENCY_PERIOD_OVERLAP);
                    }
                }
            }
        }
    }
}
