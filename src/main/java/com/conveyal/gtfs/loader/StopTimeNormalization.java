package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.model.PatternHalt;
import com.conveyal.gtfs.model.PatternLocation;
import com.conveyal.gtfs.model.PatternStop;
import com.conveyal.gtfs.model.PatternStopArea;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StopTimeNormalization {
    private static final Logger LOG = LoggerFactory.getLogger(StopTimeNormalization.class);
    final DataSource dataSource;
    final String tablePrefix;
    final Connection connection;
    final StatementTracker flexStatementTracker;
    final StatementTracker normalStatementTracker;

    StopTimeNormalization(DataSource dataSource, Connection connection, String tablePrefix) throws SQLException {
        this.dataSource = dataSource;
        // The connection already established is required so that the state of uncommitted changes are considered.
        this.connection = connection;
        this.tablePrefix = tablePrefix;
        this.flexStatementTracker = new StatementTracker(true);
        this.normalStatementTracker = new StatementTracker(false);
    }

    /**
     * For a given pattern id and starting stop sequence (inclusive), normalize all stop times to match the pattern
     * stops' travel times.
     *
     * @return number of stop times updated.
     */
    public int normalizeStopTimesForPattern(int beginWithSequence, String patternId) throws SQLException {
        JDBCTableReader<PatternStop> patternStops = new JDBCTableReader(
            Table.PATTERN_STOP,
            dataSource,
            tablePrefix + ".",
            EntityPopulator.PATTERN_STOP
        );
        JDBCTableReader<PatternLocation> patternLocations = new JDBCTableReader(
            Table.PATTERN_LOCATION,
            dataSource,
            tablePrefix + ".",
            EntityPopulator.PATTERN_LOCATION
        );
        JDBCTableReader<PatternStopArea> patternStopAreas = new JDBCTableReader(
            Table.PATTERN_STOP_AREA,
            dataSource,
            tablePrefix + ".",
            EntityPopulator.PATTERN_STOP_AREA
        );
        List<PatternHalt> patternHaltsToNormalize = new ArrayList<>();
        Iterator<PatternHalt> patternHalts = Iterators.concat(
            patternStops.getOrdered(patternId).iterator(),
            patternLocations.getOrdered(patternId).iterator(),
            patternStopAreas.getOrdered(patternId).iterator()
        );
        while (patternHalts.hasNext()) {
            PatternHalt patternHalt = patternHalts.next();
            if (patternHalt.stop_sequence >= beginWithSequence) {
                patternHaltsToNormalize.add(patternHalt);
            }
        }
        // Use PatternHalt superclass to extract shared fields to be able to compare stops and locations.
        patternHaltsToNormalize = patternHaltsToNormalize
            .stream()
            .sorted(Comparator.comparingInt(o -> (o).stop_sequence))
            .collect(Collectors.toList());
        PatternHalt firstPatternHalt = patternHaltsToNormalize.iterator().next();
        int firstStopSequence = firstPatternHalt.stop_sequence;
        Map<String, Integer> timesForTripIds = getPreviousTravelTimes(firstStopSequence, firstPatternHalt.pattern_id);

        for (Map.Entry<String, Integer> timesForTripId : timesForTripIds.entrySet()) {
            // Initialize travel time with previous stop time value.
            int cumulativeTravelTime = timesForTripId.getValue();
            for (PatternHalt patternHalt : patternHaltsToNormalize) {
                cumulativeTravelTime += cumulateTravelTime(cumulativeTravelTime, patternHalt, timesForTripId.getKey());
            }
        }
        return executeAllStatementTrackers();
    }

    private Map<String, Integer> getPreviousTravelTimes(int firstStopSequence, String patternId) throws SQLException {
        Map<String, Integer> timesForTripIds = new HashMap<>();
        String timeField = firstStopSequence > 0 ? "departure_time" : "arrival_time";
        // Prepare SQL query to determine the time that should form the basis for adding the travel time values.
        int previousStopSequence = firstStopSequence > 0 ? firstStopSequence - 1 : 0;
        String getPrevTravelTimeSql = String.format(
            "select t.trip_id, %s from %s.stop_times st, %s.trips t where stop_sequence = ? " +
                "and t.pattern_id = ? " +
                "and t.trip_id = st.trip_id",
            timeField,
            tablePrefix,
            tablePrefix
        );
        try (PreparedStatement statement = connection.prepareStatement(getPrevTravelTimeSql)) {
            statement.setInt(1, previousStopSequence);
            statement.setString(2, patternId);
            LOG.info("Get previous travel time sql: {}", statement);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                timesForTripIds.put(resultSet.getString(1), resultSet.getInt(2));
            }
        }
        return timesForTripIds;
    }

    /**
     * This MUST be called _after_ pattern reconciliation has happened. The pattern stops and pattern locations must be
     * processed based on stop sequence so the correct cumulative travel time is calculated.
     */
    public void updatePatternFrequencies(PatternReconciliation reconciliation) throws SQLException {
        // Convert to generic stops to order pattern stops/locations by stop sequence.
        List<PatternReconciliation.GenericStop> genericStops = reconciliation.getGenericStops();
        int cumulativeTravelTime = 0;
        for (PatternReconciliation.GenericStop genericStop : genericStops) {
            PatternHalt patternHalt;
            // Update stop times linked to pattern stop/location and accumulate time.
            // Default travel and dwell time behave as "linked fields" for associated stop times. In other
            // words, frequency trips in the editor must match the pattern stop travel times.
            if (genericStop.patternType == PatternReconciliation.PatternType.STOP) {
                patternHalt = reconciliation.getPatternStop(genericStop.referenceId);
            } else if (genericStop.patternType == PatternReconciliation.PatternType.LOCATION) {
                patternHalt = reconciliation.getPatternLocation(genericStop.referenceId);
            } else {
                patternHalt = reconciliation.getPatternStopArea(genericStop.referenceId);
            }
            cumulativeTravelTime += cumulateTravelTime(cumulativeTravelTime, patternHalt, null);
        }
        executeAllStatementTrackers();
    }

    /**
     * Cumulate travel time based on generic pattern halt values and update the appropriate stop times.
     */
    private int cumulateTravelTime(int cumulativeTravelTime, PatternHalt patternHalt, String tripId) throws SQLException {
        int travelTime = patternHalt.getTravelTime();
        int dwellTime = patternHalt.getDwellTime();
        updateStopTimes(
            cumulativeTravelTime,
            travelTime,
            dwellTime,
            patternHalt.pattern_id,
            patternHalt.stop_sequence,
            tripId,
            patternHalt.isFlex()
        );
        return travelTime + dwellTime;
    }

    private int executeAllStatementTrackers() throws SQLException {
        int stopTimesUpdated =
            flexStatementTracker.executeRemaining() +
            normalStatementTracker.executeRemaining();
        LOG.info("Updated {} stop times.", stopTimesUpdated);
        return stopTimesUpdated;
    }

    /**
     * Update stop time values depending on caller. If updating stop times for pattern stops, this will update the
     * arrival_time and departure_time. If updating stop times for pattern locations, this will update the
     * start_pickup_drop_off_window and end_pickup_drop_off_window.
     */
    private void updateStopTimes(
        int previousTravelTime,
        int travelTime,
        int dwellTime,
        String patternId,
        int stopSequence,
        String tripId,
        boolean isFlex
    ) throws SQLException {

        PreparedStatement statement = getPreparedStatement(isFlex, tripId);
        BatchTracker batchTracker = getBatchTracker(isFlex, tripId);
        int oneBasedIndex = 1;
        int arrivalTime = previousTravelTime + travelTime;
        statement.setInt(oneBasedIndex++, arrivalTime);
        statement.setInt(oneBasedIndex++, arrivalTime + dwellTime);
        if (tripId != null) statement.setString(oneBasedIndex++, tripId);

        // Set "where clause" with value for pattern_id and stop_sequence
        statement.setString(oneBasedIndex++, patternId);
        // In the editor, we can depend on stop_times#stop_sequence matching pattern_stop/pattern_locations#stop_sequence
        // because we normalize stop sequence values for stop times during snapshotting for the editor.
        statement.setInt(oneBasedIndex, stopSequence);
        // Log query, execute statement, and log result.
        LOG.info("Adding statement {} to tracker {}", statement, batchTracker);
        batchTracker.addBatch();
    }

    private PreparedStatement getPreparedStatement(boolean isFlex, String tripId) {
        return (isFlex)
            ? flexStatementTracker.getPreparedStatement(tripId)
            : normalStatementTracker.getPreparedStatement(tripId);
    }

    private BatchTracker getBatchTracker(boolean isFlex, String tripId) {
        return (isFlex)
            ? flexStatementTracker.getBatchTracker(tripId)
            : normalStatementTracker.getBatchTracker(tripId);
    }

    /**
     * Class used to pair prepared statements with batch trackers. It also defines prepared statements based on flex
     * and number of trips.
     */
    private class StatementTracker {
        final BatchTracker singleTripBatchTracker;

        final PreparedStatement singleTripPreparedStatement;

        final BatchTracker allTripsBatchTracker;

        final PreparedStatement allTripsPreparedStatement;

        public StatementTracker(boolean isFlex) throws SQLException {
            // Match the prepared statements with batch trackers.
            String recordTypePrefix = (isFlex) ? "Flex stop" : "Normal stop";
            String sqlForSingleTrip = (isFlex) ? getFlexStopSql(true) : getNormalStopSql(true);
            String sqlForAllTrips = (isFlex) ? getFlexStopSql(false) : getNormalStopSql(false);
            singleTripPreparedStatement = connection.prepareStatement(sqlForSingleTrip);
            singleTripBatchTracker = new BatchTracker(recordTypePrefix + ", single trip", singleTripPreparedStatement);
            allTripsPreparedStatement = connection.prepareStatement(sqlForAllTrips);
            allTripsBatchTracker = new BatchTracker(recordTypePrefix + ", all trips", allTripsPreparedStatement);
        }

        private String getNormalStopSql(boolean isSingleTrip) {
            return String.format(
                "update %s.stop_times st set arrival_time = ?, departure_time = ? from %s.trips t " +
                    "where st.trip_id = %s AND t.pattern_id = ? AND st.stop_sequence = ?",
                tablePrefix,
                tablePrefix,
                getTripIdReference(isSingleTrip)
            );
        }

        private String getFlexStopSql(boolean isSingleTrip) {
            return String.format(
                "update %s.stop_times st set start_pickup_drop_off_window = ?, end_pickup_drop_off_window = ? from %s.trips t " +
                    "where st.trip_id = %s AND t.pattern_id = ? AND st.stop_sequence = ?",
                tablePrefix,
                tablePrefix,
                getTripIdReference(isSingleTrip)
            );
        }

        private String getTripIdReference(boolean isSingleTrip) {
            return (isSingleTrip) ? "?" : "t.trip_id";
        }

        private PreparedStatement getPreparedStatement(String tripId) {
            return (tripId != null) ? singleTripPreparedStatement : allTripsPreparedStatement;
        }

        private BatchTracker getBatchTracker(String tripId) {
            return (tripId != null) ? singleTripBatchTracker : allTripsBatchTracker;
        }

        /**
         * Execute remaining prepared statements in batches.
         */
        public int executeRemaining() throws SQLException{
            return singleTripBatchTracker.executeRemaining() + allTripsBatchTracker.executeRemaining();
        }
    }
}
