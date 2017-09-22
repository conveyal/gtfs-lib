package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.util.Util;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.index.strtree.STRtree;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.conveyal.gtfs.error.NewGTFSErrorType.DUPLICATE_STOP;
import static com.conveyal.gtfs.util.Util.getCoordString;

/**
 * Find stops that are very close together.
 */
public class DuplicateStopsValidator extends FeedValidator {

    private static final double BUFFER_METERS = 2.0;

    public DuplicateStopsValidator(Feed feed, SQLErrorStorage errorStorage) {
        super(feed, errorStorage);
    }

    @Override
    public void validate () {
        // Project all stop coordinates and put them in a spatial index
        HashMap<Stop, Coordinate> projectedCoordinateForStop = new HashMap<>();
        STRtree stopSpatialIndex = new STRtree();
        for (Stop stop : feed.stops) {
            // Only validate point where vehicles stop, excluding logical "parent stations"
            if (stop.location_type != 0) continue;
            Coordinate projectedStopCoordinate = Util.projectLatLonToMeters(stop.stop_lat, stop.stop_lon);
            stopSpatialIndex.insert(new Envelope(projectedStopCoordinate), stop);
            projectedCoordinateForStop.put(stop, projectedStopCoordinate);
        }
        stopSpatialIndex.build();

        // Track which stops have already been reported in an error message so we don't report them more than once.
        Set<Stop> reportedStops = new HashSet<>();
        projectedCoordinateForStop.forEach((stop, coord) -> {
            if (reportedStops.contains(stop)) return;
            Envelope queryEnvelope = new Envelope(coord);
            queryEnvelope.expandBy(BUFFER_METERS);
            List<Stop> nearby = (List<Stop>)stopSpatialIndex.query(queryEnvelope).stream()
                    .filter(s -> !reportedStops.contains(s)).collect(Collectors.toList());
            // The nearby stops list will include at least one stop, the one for which we're performing the query.
            // We want to include that one in the referenced entities along with the duplicates.
            if (nearby.size() > 1) {
                // TODO including bad_value and info entries - settle on one or the other
                String badStopIds = nearby.stream().map(Stop::getId).filter(s -> !s.equals(stop.stop_id))
                        .map(sid -> "stopId=" + sid).collect(Collectors.joining("; "));
                NewGTFSError error = NewGTFSError.forEntity(stop, DUPLICATE_STOP).setBadValue(badStopIds);
                int i = 1;
                for (Stop nearbyStop : nearby) {
                    error.addInfo("stop_id " + i, nearbyStop.stop_id);
                    i += 1;
                }
                registerError(error);
                reportedStops.addAll(nearby);
            }
        });
    }

}
