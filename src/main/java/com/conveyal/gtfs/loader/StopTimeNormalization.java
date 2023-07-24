package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.model.Entity;
import com.conveyal.gtfs.model.PatternHalt;
import com.conveyal.gtfs.model.PatternLocation;
import com.conveyal.gtfs.model.PatternStop;
import com.conveyal.gtfs.model.PatternStopArea;
import com.google.common.collect.Iterators;
import org.apache.commons.dbutils.DbUtils;
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
    final BatchTracker normalStopTrackerForSingleTrip;
    final PreparedStatement normalStopStatementForSingleTrip;
    final BatchTracker normalStopTrackerForAllTrips;
    final PreparedStatement normalStopStatementForAllTrips;
    final BatchTracker flexStopTrackerForSingleTrip;
    final PreparedStatement flexStopStatementForSingleTrip;
    final BatchTracker flexStopTrackerForAllTrips;
    final PreparedStatement flexStopStatementForAllTrips;

    StopTimeNormalization(DataSource dataSource, Connection connection, String tablePrefix) throws SQLException {
        this.dataSource = dataSource;
        // The connection already established is required so that the state of uncommitted changes are considered.
        this.connection = connection;
        this.tablePrefix = tablePrefix;

        // Match the prepared statements with batch trackers.
        normalStopStatementForSingleTrip = connection.prepareStatement(getNormalStopSql(true));
        normalStopTrackerForSingleTrip = new BatchTracker("Normal stop, single trip", normalStopStatementForSingleTrip);
        normalStopStatementForAllTrips = connection.prepareStatement(getNormalStopSql(false));
        normalStopTrackerForAllTrips = new BatchTracker("Normal stop, all trips", normalStopStatementForAllTrips);

        flexStopStatementForSingleTrip = connection.prepareStatement(getFlexStopSql(true));
        flexStopTrackerForSingleTrip = new BatchTracker("Flex stop, single trip", flexStopStatementForSingleTrip);
        flexStopStatementForAllTrips = connection.prepareStatement(getFlexStopSql(false));
        flexStopTrackerForAllTrips = new BatchTracker("Flex stop, all trips", flexStopStatementForAllTrips);
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

    /**
     * For a given pattern id and starting stop sequence (inclusive), normalize all stop times to match the pattern
     * stops' travel times.
     *
     * @return number of stop times updated.
     */
    public int normalizeStopTimesForPattern(int beginWithSequence, String patternId) throws SQLException {
        try {
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
            // Use PatternHalt superclass to extract shared fields to be able to compare stops and locations
            patternHaltsToNormalize = patternHaltsToNormalize.stream().sorted(Comparator.comparingInt(o -> (o).stop_sequence)).collect(Collectors.toList());
            PatternHalt firstPatternHalt = patternHaltsToNormalize.iterator().next();
            int firstStopSequence = firstPatternHalt.stop_sequence;
            Map<String, Integer> timesForTripIds = getPreviousTravelTimes(firstStopSequence, firstPatternHalt.pattern_id);

            for (Map.Entry<String, Integer> timesForTripId : timesForTripIds.entrySet()) {
                // Initialize travel time with previous stop time value.
                int cumulativeTravelTime = timesForTripId.getValue();
                for (PatternHalt patternHalt : patternHaltsToNormalize) {
                    if (patternHalt instanceof PatternStop) {
                        cumulativeTravelTime += updateStopTimesForPatternStop(
                            (PatternStop) patternHalt,
                            cumulativeTravelTime,
                            timesForTripId.getKey()
                        );
                    } else if (patternHalt instanceof PatternLocation) {
                        PatternLocation patternLocation = (PatternLocation) patternHalt;
                        cumulativeTravelTime += updateStopTimesForPatternLocationOrPatternStopArea(
                            patternLocation.flex_default_travel_time,
                            patternLocation.flex_default_zone_time,
                            patternLocation.pattern_id,
                            patternLocation.stop_sequence,
                            cumulativeTravelTime,
                            timesForTripId.getKey()
                        );
                    } else if (patternHalt instanceof PatternStopArea) {
                        PatternStopArea patternStopArea = (PatternStopArea) patternHalt;
                        cumulativeTravelTime += updateStopTimesForPatternLocationOrPatternStopArea(
                            patternStopArea.flex_default_travel_time,
                            patternStopArea.flex_default_zone_time,
                            patternStopArea.pattern_id,
                            patternStopArea.stop_sequence,
                            cumulativeTravelTime,
                            timesForTripId.getKey()
                        );
                    } else {
                        LOG.warn("Pattern with ID {} contained a halt that wasn't a normal or flex stop!", patternId);
                    }
                }
            }
            int stopTimesUpdated = executeAllBatchTrackers();
            connection.commit();
            LOG.info("Update {} stop times.", stopTimesUpdated);
            return stopTimesUpdated;
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            DbUtils.closeQuietly(connection);
        }
    }

    private Map<String, Integer> getPreviousTravelTimes(int firstStopSequence, String patternId) throws SQLException {
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
        Map<String, Integer> timesForTripIds = new HashMap<>();
        try (PreparedStatement statement = dataSource.getConnection().prepareStatement(getPrevTravelTimeSql)) {
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
            // Update stop times linked to pattern stop/location and accumulate time.
            // Default travel and dwell time behave as "linked fields" for associated stop times. In other
            // words, frequency trips in the editor must match the pattern stop travel times.
            if (genericStop.patternType == PatternReconciliation.PatternType.STOP) {
                cumulativeTravelTime +=
                    updateStopTimesForPatternStop(
                        reconciliation.getPatternStop(genericStop.referenceId),
                        cumulativeTravelTime,
                        null
                    );
            } else if (genericStop.patternType == PatternReconciliation.PatternType.LOCATION) {
                PatternLocation patternLocation = reconciliation.getPatternLocation(genericStop.referenceId);
                cumulativeTravelTime += updateStopTimesForPatternLocationOrPatternStopArea(
                    patternLocation.flex_default_travel_time,
                    patternLocation.flex_default_zone_time,
                    patternLocation.pattern_id,
                    patternLocation.stop_sequence,
                    cumulativeTravelTime,
                    null
                );
            } else {
                PatternStopArea patternStopArea = reconciliation.getPatternStopArea(genericStop.referenceId);
                cumulativeTravelTime += updateStopTimesForPatternLocationOrPatternStopArea(
                    patternStopArea.flex_default_travel_time,
                    patternStopArea.flex_default_zone_time,
                    patternStopArea.pattern_id,
                    patternStopArea.stop_sequence,
                    cumulativeTravelTime,
                    null
                );
            }
        }
        int stopTimesUpdated = executeAllBatchTrackers();
        LOG.info("Updated {} stop times.", stopTimesUpdated);
    }

    private int executeAllBatchTrackers() throws SQLException {
        return
            normalStopTrackerForAllTrips.executeRemaining() +
            normalStopTrackerForSingleTrip.executeRemaining() +
            flexStopTrackerForAllTrips.executeRemaining() +
            flexStopTrackerForSingleTrip.executeRemaining();
    }

    /**
     * Updates the stop times that reference the specified pattern stop.
     *
     * @param patternStop        the pattern stop for which to update stop times
     * @param previousTravelTime the travel time accumulated up to the previous stop_time's departure time (or the
     *                           previous pattern stop's dwell time)
     * @return the travel and dwell time added by this pattern stop
     * @throws SQLException
     */
    private int updateStopTimesForPatternStop(PatternStop patternStop, int previousTravelTime, String tripId)
        throws SQLException {

        int travelTime = patternStop.default_travel_time == Entity.INT_MISSING ? 0 : patternStop.default_travel_time;
        int dwellTime = patternStop.default_dwell_time == Entity.INT_MISSING ? 0 : patternStop.default_dwell_time;
        updateStopTimes(
            previousTravelTime,
            travelTime,
            dwellTime,
            patternStop.pattern_id,
            patternStop.stop_sequence,
            tripId,
            false
        );
        return travelTime + dwellTime;
    }

    /**
     * Updates the stop times that reference the specified pattern location or pattern stop area.
     *
     * @return the travel and dwell time added by this pattern.
     * @throws SQLException
     */
    private int updateStopTimesForPatternLocationOrPatternStopArea(
        int flexDefaultTravelTime,
        int flexDefaultZoneTime,
        String patternId,
        int stopSequence,
        int previousTravelTime,
        String tripId
    ) throws SQLException {

        int travelTime = flexDefaultTravelTime == Entity.INT_MISSING ? 0 : flexDefaultTravelTime;
        int dwellTime = flexDefaultZoneTime == Entity.INT_MISSING ? 0 : flexDefaultZoneTime;
        updateStopTimes(
            previousTravelTime,
            travelTime,
            dwellTime,
            patternId,
            stopSequence,
            tripId,
            true
        );
        return travelTime + dwellTime;
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
        LOG.info("Updating stop time {}", statement);
        batchTracker.addBatch();
    }

    private PreparedStatement getPreparedStatement(boolean isFlex, String tripId) {
        if (isFlex) {
            return (tripId != null) ? this.flexStopStatementForSingleTrip : this.flexStopStatementForAllTrips;
        } else {
            return (tripId != null) ? this.normalStopStatementForSingleTrip : this.normalStopStatementForAllTrips;
        }
    }

    private BatchTracker getBatchTracker(boolean isFlex, String tripId) {
        if (isFlex) {
            return (tripId != null) ? this.flexStopTrackerForSingleTrip : this.flexStopTrackerForAllTrips;
        } else {
            return (tripId != null) ? this.normalStopTrackerForSingleTrip : this.normalStopTrackerForAllTrips;
        }
    }

}
