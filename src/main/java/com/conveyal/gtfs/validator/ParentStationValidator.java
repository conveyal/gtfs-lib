package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.model.Stop;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static com.conveyal.gtfs.error.NewGTFSErrorType.REFERENTIAL_INTEGRITY;

/**
 * Find stop#parent_station values that reference non-existent stop_ids. Unfortunately, we cannot perform this check
 * while the stops.txt table is being loaded because we do not yet have the full set of stop_ids available to check
 * parent_station values against.
 */
public class ParentStationValidator extends FeedValidator {

    public ParentStationValidator(Feed feed, SQLErrorStorage errorStorage) {
        super(feed, errorStorage);
    }

    @Override
    public void validate () {
        Multimap<String, Stop> stopsForparentStations = HashMultimap.create();
        Set<String> stopIds = new HashSet<>();
        for (Stop stop : feed.stops) {
            // Collect all stop_ids found in feed.
            stopIds.add(stop.stop_id);
            // Collect all non-null parent_station values.
            if (stop.parent_station != null) {
                stopsForparentStations.put(stop.parent_station, stop);
            }
        }
        // Find parent_station values that do not reference a valid stop_id from feed.
        Sets.SetView<String> badReferences = Sets.difference(stopsForparentStations.keySet(), stopIds);
        for (String parentStation : badReferences) {
            // For any bad parent_station ref (this could be more than one stop), add an error to the error storage.
            Collection<Stop> stops = stopsForparentStations.get(parentStation);
            for (Stop stop : stops) {
                registerError(
                    NewGTFSError
                        .forLine(Table.STOPS, stop.id, REFERENTIAL_INTEGRITY, parentStation)
                        .setEntityId(stop.stop_id)
                );
            }
        }
    }

}
