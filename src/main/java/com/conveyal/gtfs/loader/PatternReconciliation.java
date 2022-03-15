package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.model.PatternLocation;
import com.conveyal.gtfs.model.PatternStop;
import com.conveyal.gtfs.model.StopTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PatternReconciliation {

    private static final Logger LOG = LoggerFactory.getLogger(PatternReconciliation.class);
    private static final String RECONCILE_STOPS_ERROR_MSG = "Changes to trip pattern stops or locations must be made one at a time if pattern contains at least one trip.";

    /**
     * Enum containing available pattern types.
     */
    private enum PatternType {
        LOCATION, STOP
    }

    /**
     * Reconcile pattern stops.
     */
    public static void reconcilePatternStops(
        String tablePrefix,
        String patternId,
        List<PatternStop> patternStops,
        Connection connection
    ) throws SQLException {
        List<Pattern> patterns = patternStops.stream().map(Pattern::new).collect(Collectors.toList());
        reconcilePattern(tablePrefix, patternId, patterns, connection, PatternType.STOP);
    }

    /**
     * Reconcile pattern locations.
     */
    public static void reconcilePatternLocations(
        String tablePrefix,
        String patternId,
        List<PatternLocation> patternLocations,
        Connection connection
    ) throws SQLException {
        List<Pattern> patterns = patternLocations.stream().map(Pattern::new).collect(Collectors.toList());
        reconcilePattern(tablePrefix, patternId, patterns, connection, PatternType.LOCATION);
    }

    /**
     * Update the trip pattern locations and the associated stop times.
     *
     * We assume only one location has changed, either it's been removed, added or moved. The only other case that is
     * permitted is adding a set of stops to the end of the original list. These conditions are evaluated by simply
     * checking the lengths of the original and new pattern stops (and ensuring that stop IDs remain the same where
     * required).
     *
     * If the change to pattern stops does not satisfy one of these cases, fail the update operation.
     *
     */
    private static void reconcilePattern(
        String tablePrefix,
        String patternId,
        List<Pattern> patterns,
        Connection connection,
        PatternType patternType
    ) throws SQLException {

        LOG.info("Reconciling pattern for pattern ID={} for pattern type={}", patternId, patternType);
        if (patterns.isEmpty()) return;

        List<String> tripsForPattern = getTripIdsForPatternId(tablePrefix, patternId, connection);
        if (tripsForPattern.size() == 0) {
            // If there are no trips for the pattern, there is no need to reconcile stop times to modified pattern stops.
            // This permits the creation of patterns without stops, reversing the stops on existing patterns, and
            // duplicating patterns.
            // For new patterns, this short circuit is required to prevent the transposition conditional check from
            // throwing an IndexOutOfBoundsException when it attempts to access index 0 of a list with no items.
            return;
        }

        List<String> originalPatternIds = getOriginalPatternIds(tablePrefix, patternId, patternType, connection);

        // Prepare SQL fragment to filter for all stop times for all trips on a certain pattern.
        String joinToTrips = String.format("%s.trips.trip_id = %s.stop_times.trip_id AND %s.trips.pattern_id = '%s'",
            tablePrefix, tablePrefix, tablePrefix, patternId);

        // ADDITIONS (IF DIFF == 1)
        if (originalPatternIds.size() == patterns.size() - 1) {
            // We have an addition; find it.
            int differenceLocation = -1;
            for (int i = 0; i < patterns.size(); i++) {
                if (differenceLocation != -1) {
                    // we've already found the addition
                    Object pattern = patterns.get(i + 1);
                    if (i < originalPatternIds.size() &&
                        !originalPatternIds.get(i).equals(patterns.get(i + 1).referenceId)
                    ) {
                        // there's another difference, which we weren't expecting
                        throw new IllegalStateException("Multiple differences found when trying to detect stop addition");
                    }
                }
                // if we've reached where one trip has an extra stop, or if the stops at this position differ
                else if (i == patterns.size() - 1 || !originalPatternIds.get(i).equals(patterns.get(i).referenceId)) {
                    // we have found the difference
                    differenceLocation = i;
                }
            }
            // Increment sequences for stops that follow the inserted location (including the stop at the changed index).
            // NOTE: This should happen before the blank stop time insertion for logical consistency.
            String updateSql = String.format(
                "update %s.stop_times set stop_sequence = stop_sequence + 1 from %s.trips where stop_sequence >= %d AND %s",
                tablePrefix,
                tablePrefix,
                differenceLocation,
                joinToTrips
            );
            LOG.info(updateSql);
            PreparedStatement updateStatement = connection.prepareStatement(updateSql);
            int updated = updateStatement.executeUpdate();
            LOG.info("Updated {} stop times", updated);

            // Insert a skipped stop at the difference location
            insertBlankStopTimes(
                tablePrefix,
                tripsForPattern,
                patterns,
                differenceLocation,
                1,
                connection
            );
        } else if (originalPatternIds.size() == patterns.size() + 1) {
            // DELETIONS
            // We have a deletion; find it
            int differenceLocation = -1;
            for (int i = 0; i < originalPatternIds.size(); i++) {
                if (differenceLocation != -1) {
                    if (!originalPatternIds.get(i).equals(patterns.get(i - 1).referenceId)) {
                        // There is another difference, which we were not expecting
                        throw new IllegalStateException("Multiple differences found when trying to detect stop removal");
                    }
                } else if (
                    i == originalPatternIds.size() - 1 ||
                    !originalPatternIds.get(i).equals(patterns.get(i).referenceId)
                ) {
                    // We've reached the end and the only difference is length (so the last stop is the different one)
                    // or we've found the difference.
                    differenceLocation = i;
                }
            }
            // Delete stop at difference location
            String deleteSql = String.format(
                "delete from %s.stop_times using %s.trips where stop_sequence = %d AND %s",
                tablePrefix,
                tablePrefix,
                differenceLocation,
                joinToTrips
            );
            LOG.info(deleteSql);
            PreparedStatement deleteStatement = connection.prepareStatement(deleteSql);
            // Decrement all stops with sequence greater than difference location
            String updateSql = String.format(
                "update %s.stop_times set stop_sequence = stop_sequence - 1 from %s.trips where stop_sequence > %d AND %s",
                tablePrefix,
                tablePrefix,
                differenceLocation,
                joinToTrips
            );
            LOG.info(updateSql);
            PreparedStatement updateStatement = connection.prepareStatement(updateSql);
            int deleted = deleteStatement.executeUpdate();
            int updated = updateStatement.executeUpdate();
            LOG.info("Deleted {} stop times, updated sequence for {} stop times", deleted, updated);
        }

        // TRANSPOSITIONS
        else if (originalPatternIds.size() == patterns.size()) {
            // Imagine the trip patterns pictured below (where . is a stop, and lines indicate the same stop)
            // the original trip pattern is on top, the new below
            // . . . . . . . .
            // | |  \ \ \  | |
            // * * * * * * * *
            // also imagine that the two that are unmarked are the same
            // (the limitations of ascii art, this is prettier on my whiteboard)
            // There are three regions: the beginning and end, where stopSequences are the same, and the middle, where they are not
            // The same is true of trips where stops were moved backwards

            // find the left bound of the changed region
            int firstDifferentIndex = 0;
            while (originalPatternIds.get(firstDifferentIndex).equals(patterns.get(firstDifferentIndex).referenceId)) {
                firstDifferentIndex++;

                if (firstDifferentIndex == originalPatternIds.size())
                    // trip patterns do not differ at all, nothing to do
                    return;
            }

            // find the right bound of the changed region
            int lastDifferentIndex = originalPatternIds.size() - 1;
            while (originalPatternIds.get(lastDifferentIndex).equals(patterns.get(lastDifferentIndex).referenceId)) {
                lastDifferentIndex--;
            }

            // TODO: write a unit test for this
            if (firstDifferentIndex == lastDifferentIndex) {
                throw new IllegalStateException(
                    "Pattern stop substitutions are not supported, region of difference must have length > 1.");
            }
            String conditionalUpdate;

            // figure out whether a stop was moved left or right
            // note that if the stop was only moved one position, it's impossible to tell, and also doesn't matter,
            // because the requisite operations are equivalent
            int from, to;
            List<String> newReferenceIds= getPatternReferenceIds(patterns);
            // Ensure that only a single stop has been moved (i.e. verify stop IDs inside changed region remain unchanged)
            if (originalPatternIds.get(firstDifferentIndex).equals(patterns.get(lastDifferentIndex).referenceId)) {
                // Stop was moved from beginning of changed region to end of changed region (-->)
                from = firstDifferentIndex;
                to = lastDifferentIndex;
                verifyInteriorStopsAreUnchanged(originalPatternIds, newReferenceIds, firstDifferentIndex, lastDifferentIndex);
                conditionalUpdate = String.format("update %s.stop_times set stop_sequence = case " +
                        // if sequence = fromIndex, update to toIndex.
                        "when stop_sequence = %d then %d " +
                        // if sequence is greater than fromIndex and less than or equal to toIndex, decrement
                        "when stop_sequence > %d AND stop_sequence <= %d then stop_sequence - 1 " +
                        // Otherwise, sequence remains untouched
                        "else stop_sequence " +
                        "end " +
                        "from %s.trips where %s",
                    tablePrefix, from, to, from, to, tablePrefix, joinToTrips);
            } else if (patterns.get(firstDifferentIndex).referenceId.equals(originalPatternIds.get(lastDifferentIndex))) {
                // Stop was moved from end of changed region to beginning of changed region (<--)
                from = lastDifferentIndex;
                to = firstDifferentIndex;
                verifyInteriorStopsAreUnchanged(originalPatternIds, newReferenceIds, firstDifferentIndex, lastDifferentIndex);
                conditionalUpdate = String.format("update %s.stop_times set stop_sequence = case " +
                        // if sequence = fromIndex, update to toIndex.
                        "when stop_sequence = %d then %d " +
                        // if sequence is less than fromIndex and greater than or equal to toIndex, increment
                        "when stop_sequence < %d AND stop_sequence >= %d then stop_sequence + 1 " +
                        // Otherwise, sequence remains untouched
                        "else stop_sequence " +
                        "end " +
                        "from %s.trips where %s",
                    tablePrefix, from, to, from, to, tablePrefix, joinToTrips);
            } else {
                throw new IllegalStateException("not a simple, single move!");
            }

            // Update the stop sequences for the stop that was moved and the other stops within the changed region.
            PreparedStatement updateStatement = connection.prepareStatement(conditionalUpdate);
            LOG.info(updateStatement.toString());
            int updated = updateStatement.executeUpdate();
            LOG.info("Updated {} stop_times.", updated);
        }
        // CHECK IF SET OF STOPS ADDED TO END OF ORIGINAL LIST
        else if (originalPatternIds.size() < patterns.size()) {
            // find the left bound of the changed region to check that no stops have changed in between
            int firstDifferentIndex = 0;
            while (
                firstDifferentIndex < originalPatternIds.size() &&
                originalPatternIds.get(firstDifferentIndex).equals(patterns.get(firstDifferentIndex).referenceId)
            ) {
                firstDifferentIndex++;
            }
            if (firstDifferentIndex != originalPatternIds.size())
                throw new IllegalStateException("When adding multiple stops to patterns, new stops must all be at the end");

            // insert a skipped stop for each new element in newStops
            int stopsToInsert = patterns.size() - firstDifferentIndex;
            // FIXME: Should we be inserting blank stop times at all?  Shouldn't these just inherit the arrival times
            // from the pattern stops?
            LOG.info("Adding {} stop times to existing {} stop times. Starting at {}",
                stopsToInsert,
                originalPatternIds.size(),
                firstDifferentIndex
            );
            insertBlankStopTimes(
                tablePrefix,
                tripsForPattern,
                patterns,
                firstDifferentIndex,
                stopsToInsert,
                connection
            );
        } else {
            // ANY OTHER TYPE OF MODIFICATION IS NOT SUPPORTED
            throw new IllegalStateException(RECONCILE_STOPS_ERROR_MSG);
        }
    }

    /**
     * Collect the original list of pattern ids. This will either be from pattern stops or pattern locations depending
     * on the pattern type.
     */
    private static List<String> getOriginalPatternIds(
        String tablePrefix,
        String patternId,
        PatternType patternType,
        Connection connection
    ) throws SQLException {
        String sql;
        switch (patternType) {
            case LOCATION:
                sql = String.format("select location_id from %s.pattern_locations where pattern_id = ? order by stop_sequence",
                    tablePrefix);
                break;
            case STOP:
                sql = String.format("select stop_id from %s.pattern_stops where pattern_id = ? order by stop_sequence",
                    tablePrefix);
                break;
            default:
                String error = String.format("Unknown pattern type %s. Unable to get original pattern ids.", patternType);
                LOG.error(error);
                throw new SQLException(error);
        }
        PreparedStatement getLocationsStatement = connection.prepareStatement(sql);
        getLocationsStatement.setString(1, patternId);
        LOG.info(getLocationsStatement.toString());
        ResultSet locationsResults = getLocationsStatement.executeQuery();
        List<String> originalPatternIds = new ArrayList<>();
        while (locationsResults.next()) {
            originalPatternIds.add(locationsResults.getString(1));
        }
        return originalPatternIds;
    }

    /**
     * Collect all trip IDs so that we can insert new stop times (with the appropriate trip ID value) if a pattern
     * is added.
     */
    private static List<String> getTripIdsForPatternId(
        String tablePrefix,
        String patternId,
        Connection connection
    ) throws SQLException {
        String getTripIdsSql = String.format("select trip_id from %s.trips where pattern_id = ?", tablePrefix);
        PreparedStatement getTripsStatement = connection.prepareStatement(getTripIdsSql);
        getTripsStatement.setString(1, patternId);
        ResultSet tripsResults = getTripsStatement.executeQuery();
        List<String> tripsForPattern = new ArrayList<>();
        while (tripsResults.next()) {
            tripsForPattern.add(tripsResults.getString(1));
        }
        return tripsForPattern;
    }

    /**
     * Get a list of pattern reference ids.
     */
    private static List<String> getPatternReferenceIds(List<Pattern> patterns) {
        return patterns.stream().map(pattern -> pattern.referenceId).collect(Collectors.toList());
    }

    /**
     * You must call this method after updating sequences for any stop times following the starting stop sequence to
     * avoid overwriting these other stop times.
     */
    private static void insertBlankStopTimes(
        String tablePrefix,
        List<String> tripIds,
        List<Pattern> patterns,
        int startingStopSequence,
        int stopTimesToAdd,
        Connection connection
    ) throws SQLException {
        if (tripIds.isEmpty()) {
            // There is no need to insert blank stop times if there are no trips for the pattern.
            return;
        }
        String insertSql = Table.STOP_TIMES.generateInsertSql(tablePrefix, true);
        PreparedStatement insertStatement = connection.prepareStatement(insertSql);
        int totalRowsUpdated = 0;
        // Create a new stop time for each sequence value (times each trip ID) that needs to be inserted.
        for (int i = startingStopSequence; i < stopTimesToAdd + startingStopSequence; i++) {
            StopTime stopTime = patterns.get(i).stopTime;
            stopTime.stop_sequence = i;
            // Update stop time with each trip ID and add to batch.
            for (String tripId : tripIds) {
                stopTime.trip_id = tripId;
                stopTime.setStatementParameters(insertStatement, true);
                insertStatement.addBatch();
                int[] rowsUpdated = insertStatement.executeBatch();
                totalRowsUpdated += rowsUpdated.length;
            }
        }
        int[] rowsUpdated = insertStatement.executeBatch();
        totalRowsUpdated += rowsUpdated.length;
        LOG.info("{} blank stop times inserted", totalRowsUpdated);
    }

    /**
     * Check the stops in the changed region to ensure they remain in the same order. If not, throw an exception to
     * cancel the transaction.
     */
    private static void verifyInteriorStopsAreUnchanged(
        List<String> originalStopIds,
        List<String> newStopIds,
        int firstDifferentIndex,
        int lastDifferentIndex
    ) {
        //Stops mapped to list of stop IDs simply for easier viewing/comparison with original IDs while debugging with
        // breakpoints.
        // Determine the bounds of the region that should be identical between the two lists.
        int endRegion = lastDifferentIndex - 1;
        for (int i = firstDifferentIndex; i <= endRegion; i++) {
            // Shift index when selecting stop from original list to account for displaced stop.
            int shiftedIndex = i + 1;
            String newStopId = newStopIds.get(i);
            String originalStopId = originalStopIds.get(shiftedIndex);
            if (!newStopId.equals(originalStopId)) {
                // If stop ID for new stop at the given index does not match the original stop ID, the order of at least
                // one stop within the changed region has been changed, which is illegal according to the rule enforcing
                // only a single addition, deletion, or transposition per update.
                throw new IllegalStateException(RECONCILE_STOPS_ERROR_MSG);
            }
        }
    }

    /**
     * Generic pattern class use to hold either pattern stops or pattern location derived data.
     */
    private static class Pattern {
        public String referenceId;
        StopTime stopTime;
        public Pattern(PatternLocation patternLocation) {
            referenceId = patternLocation.location_id;
            stopTime = new StopTime();
            stopTime.stop_id = patternLocation.location_id;
            stopTime.drop_off_type = patternLocation.drop_off_type;
            stopTime.pickup_type = patternLocation.pickup_type;
            stopTime.timepoint = patternLocation.timepoint;
            stopTime.continuous_drop_off = patternLocation.continuous_drop_off;
            stopTime.continuous_pickup = patternLocation.continuous_pickup;
            stopTime.pickup_booking_rule_id = patternLocation.pickup_booking_rule_id;
            stopTime.drop_off_booking_rule_id = patternLocation.drop_off_booking_rule_id;
        }

        public Pattern(PatternStop patternStop) {
            referenceId = patternStop.stop_id;
            stopTime = new StopTime();
            stopTime.stop_id = patternStop.stop_id;
            stopTime.drop_off_type = patternStop.drop_off_type;
            stopTime.pickup_type = patternStop.pickup_type;
            stopTime.timepoint = patternStop.timepoint;
            stopTime.shape_dist_traveled = patternStop.shape_dist_traveled;
            stopTime.continuous_drop_off = patternStop.continuous_drop_off;
            stopTime.continuous_pickup = patternStop.continuous_pickup;
        }
    }
}
