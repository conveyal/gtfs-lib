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

/**
 * Update a trip pattern stops/locations and associated stop times.
 */
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
        List<GenericStop> genericStops = patternStops.stream().map(GenericStop::new).collect(Collectors.toList());
        reconcilePattern(tablePrefix, patternId, genericStops, connection, PatternType.STOP);
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
        List<GenericStop> genericStops = patternLocations.stream().map(GenericStop::new).collect(Collectors.toList());
        reconcilePattern(tablePrefix, patternId, genericStops, connection, PatternType.LOCATION);
    }

    /**
     * We assume only one stop time has changed, either it's been removed, added or moved. The only other case that is
     * permitted is adding a set of generic stops to the end of the original list. These conditions are evaluated by
     * simply checking the lengths of the original and generic stops (and ensuring that stop IDs remain
     * the same where required). If the change to the generic stops does not satisfy one of these cases, fail the
     * update operation.
     *
     */
    private static void reconcilePattern(
        String tablePrefix,
        String patternId,
        List<GenericStop> genericStops,
        Connection connection,
        PatternType patternType
    ) throws SQLException {
        LOG.info("Reconciling pattern for pattern ID={} for pattern type={}", patternId, patternType);
        if (genericStops.isEmpty()) {
            return;
        }
        List<String> tripsForPattern = getTripIdsForPatternId(tablePrefix, patternId, connection);
        if (tripsForPattern.size() == 0) {
            // No trips for the pattern, there is no need to reconcile stop times to modified patterns.
            return;
        }
        List<String> originalGenericStopIds = getOriginalGenericStopIds(tablePrefix, patternId, patternType, connection);
        // Prepare SQL fragment to filter for all stop times for all trips on a certain pattern.
        String joinToTrips = String.format("%s.trips.trip_id = %s.stop_times.trip_id AND %s.trips.pattern_id = '%s'",
            tablePrefix, tablePrefix, tablePrefix, patternId);
        int sizeDiff = originalGenericStopIds.size() - genericStops.size();
        if (sizeDiff == -1) {
            addOneStopToAPattern(connection, tablePrefix, originalGenericStopIds, genericStops, joinToTrips, tripsForPattern);
        } else if (sizeDiff == 1) {
            deleteOneStopFromAPattern(connection, tablePrefix, originalGenericStopIds, genericStops, joinToTrips);
        } else if (sizeDiff == 0) {
            transposeTwoStopsInAPattern(connection, tablePrefix, originalGenericStopIds, genericStops, joinToTrips);
        } else if (sizeDiff < -1) {
            addOneOrMoreStopsToEndOfPattern(connection, tablePrefix, originalGenericStopIds, genericStops, tripsForPattern);
        } else {
            // Any other type of modification is not supported.
            throw new IllegalStateException(RECONCILE_STOPS_ERROR_MSG);
        }
    }

    /**
     * Add a single generic stop to a pattern.
     */
    private static void addOneStopToAPattern(
        Connection connection,
        String tablePrefix,
        List<String> originalGenericStopIds,
        List<GenericStop> genericStops,
        String joinToTrips,
        List<String> tripsForPattern
    ) throws SQLException {
        // We have an addition; find it.
        int differenceLocation = checkForGenericStopDifference(originalGenericStopIds, genericStops);
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
            genericStops,
            differenceLocation,
            1,
            connection
        );
    }

    /**
     * Find and delete one generic stop from within a pattern.
     */
    private static void deleteOneStopFromAPattern(
        Connection connection,
        String tablePrefix,
        List<String> originalGenericStopIds,
        List<GenericStop> genericStops,
        String joinToTrips
    ) throws SQLException {
        // We have a deletion; find it.
        int differenceLocation = checkForOriginalPatternDifference(originalGenericStopIds, genericStops);
        // Delete stop at difference location.
        String deleteSql = String.format(
            "delete from %s.stop_times using %s.trips where stop_sequence = %d AND %s",
            tablePrefix,
            tablePrefix,
            differenceLocation,
            joinToTrips
        );
        LOG.info(deleteSql);
        PreparedStatement deleteStatement = connection.prepareStatement(deleteSql);
        // Decrement all stops with sequence greater than difference location.
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

    /**
     * Transpose two generic stops within a pattern.
     *
     * Imagine the trip patterns pictured below (where . is a stop, and lines indicate the same stop)
     * the original trip pattern is on top, the new below:
     * . . . . . . . .
     * | |  \ \ \  | |
     * * * * * * * * *
     * Also imagine that the two that are unmarked are the same (the limitations of ascii art, this is prettier
     * on my whiteboard). There are three regions: the beginning and end, where stopSequences are the same, and
     * the middle, where they are not. The same is true of trips where stops were moved backwards.
     */
    private static void transposeTwoStopsInAPattern(
        Connection connection,
        String tablePrefix,
        List<String> originalGenericStopIds,
        List<GenericStop> genericStops,
        String joinToTrips
    ) throws SQLException {
        // Find the left bound of the changed region.
        int firstDifferentIndex = 0;
        while (originalGenericStopIds.get(firstDifferentIndex).equals(genericStops.get(firstDifferentIndex).referenceId)) {
            firstDifferentIndex++;
            if (firstDifferentIndex == originalGenericStopIds.size())
                // trip patterns do not differ at all, nothing to do
                return;
        }
        // Find the right bound of the changed region.
        int lastDifferentIndex = originalGenericStopIds.size() - 1;
        while (originalGenericStopIds.get(lastDifferentIndex).equals(genericStops.get(lastDifferentIndex).referenceId)) {
            lastDifferentIndex--;
        }
        // TODO: write a unit test for this
        if (firstDifferentIndex == lastDifferentIndex) {
            throw new IllegalStateException(
                "Pattern substitutions are not supported. Swapping out a stop for another is prohibited.");
        }
        String arithmeticOperator;
        // Figure out whether a stop was moved left or right.
        // Note: If the stop was only moved one position, it's impossible to tell, and also doesn't matter,
        // because the requisite operations are equivalent
        int from, to;
        List<String> newReferenceIds = getPatternReferenceIds(genericStops);
        // Ensure that only a single stop has been moved (i.e. verify stop IDs inside changed region remain unchanged)
        if (originalGenericStopIds.get(firstDifferentIndex).equals(genericStops.get(lastDifferentIndex).referenceId)) {
            // Stop was moved from beginning of changed region to end of changed region (-->)
            from = firstDifferentIndex;
            to = lastDifferentIndex;
            // If sequence is greater than fromIndex and less than or equal to toIndex, decrement.
            arithmeticOperator = "-";
        } else if (genericStops.get(firstDifferentIndex).referenceId.equals(originalGenericStopIds.get(lastDifferentIndex))) {
            // Stop was moved from end of changed region to beginning of changed region (<--)
            from = lastDifferentIndex;
            to = firstDifferentIndex;
            // If sequence is less than fromIndex and greater than or equal to toIndex, increment.
            arithmeticOperator = "+";
        } else {
            throw new IllegalStateException("not a simple, single move!");
        }
        verifyInteriorStopsAreUnchanged(originalGenericStopIds, newReferenceIds, firstDifferentIndex, lastDifferentIndex);
        String conditionalUpdate = String.format("update %s.stop_times set stop_sequence = case " +
                // if sequence = fromIndex, update to toIndex.
                "when stop_sequence = %d then %d " +
                // increment or decrement stop_sequence value.
                "when stop_sequence between %d AND %d then stop_sequence %s 1 " +
                // Otherwise, sequence remains untouched
                "else stop_sequence " +
                "end " +
                "from %s.trips where %s",
            tablePrefix, from, to, from, to, arithmeticOperator, tablePrefix, joinToTrips);
        // Update the stop sequences for the stop that was moved and the other stops within the changed region.
        PreparedStatement updateStatement = connection.prepareStatement(conditionalUpdate);
        LOG.info(updateStatement.toString());
        int updated = updateStatement.executeUpdate();
        LOG.info("Updated {} stop_times.", updated);
    }

    /**
     * Add one or more generic stops to the end of a pattern.
     */
    private static void addOneOrMoreStopsToEndOfPattern(
        Connection connection,
        String tablePrefix,
        List<String> originalGenericStopIds,
        List<GenericStop> genericStops,
        List<String> tripsForPattern
    ) throws SQLException {
        // find the left bound of the changed region to check that no stops have changed in between
        int firstDifferentIndex = 0;
        while (
            firstDifferentIndex < originalGenericStopIds.size() &&
            originalGenericStopIds.get(firstDifferentIndex).equals(genericStops.get(firstDifferentIndex).referenceId)
        ) {
            firstDifferentIndex++;
        }
        if (firstDifferentIndex != originalGenericStopIds.size())
            throw new IllegalStateException("When adding multiple stops to patterns, new stops must all be at the end");

        // insert a skipped stop for each new element in newStops
        int stopsToInsert = genericStops.size() - firstDifferentIndex;
        // FIXME: Should we be inserting blank stop times at all?  Shouldn't these just inherit the arrival times
        // from the pattern stops?
        LOG.info("Adding {} stop times to existing {} stop times. Starting at {}",
            stopsToInsert,
            originalGenericStopIds.size(),
            firstDifferentIndex
        );
        insertBlankStopTimes(
            tablePrefix,
            tripsForPattern,
            genericStops,
            firstDifferentIndex,
            stopsToInsert,
            connection
        );
    }

    /**
     * Check the new generic stop ids for differences against the original generic stop ids. If only one change has
     * been made (expected behaviour) return the index with the difference. If more than one difference is found
     * throw an exception.
     */
    private static int checkForGenericStopDifference(List<String> originalStopIds, List<GenericStop> genericStops) {
        int differenceLocation = -1;
        for (int i = 0; i < genericStops.size(); i++) {
            if (differenceLocation != -1) {
                if (
                    i < originalStopIds.size() &&
                    !originalStopIds.get(i).equals(genericStops.get(i + 1).referenceId)
                ) {
                    // The addition has already been found and there's another difference, which we weren't expecting
                    throw new IllegalStateException("Multiple differences found when trying to detect stop addition");
                }
            } else if (
                i == genericStops.size() - 1 ||
                !originalStopIds.get(i).equals(genericStops.get(i).referenceId)
            ) {
                // if we've reached where one trip has an extra stop, or if the stops at this position differ
                differenceLocation = i;
            }
        }
        return differenceLocation;
    }

    /**
     * Check the original generic stop ids for differences against the new generic stop ids. If only one change has
     * been made (expected behaviour) return the index with the difference. If more than one difference is found
     * throw an exception.
     */
    private static int checkForOriginalPatternDifference(List<String> originalStopIds, List<GenericStop> genericStops) {
        int differenceLocation = -1;
        for (int i = 0; i < originalStopIds.size(); i++) {
            if (differenceLocation != -1) {
                if (!originalStopIds.get(i).equals(genericStops.get(i - 1).referenceId)) {
                    // There is another difference, which we were not expecting.
                    throw new IllegalStateException("Multiple differences found when trying to detect stop removal.");
                }
            } else if (
                i == originalStopIds.size() - 1 ||
                !originalStopIds.get(i).equals(genericStops.get(i).referenceId)
            ) {
                // We've reached the end and the only difference is length (so the last stop is the different one)
                // or we've found the difference.
                differenceLocation = i;
            }
        }
        return differenceLocation;
    }

    /**
     * Collect the original list of generic stop ids. This will either be from pattern stops or pattern locations
     * depending on the pattern type.
     */
    private static List<String> getOriginalGenericStopIds(
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
        List<String> originalGenericStopIds = new ArrayList<>();
        while (locationsResults.next()) {
            originalGenericStopIds.add(locationsResults.getString(1));
        }
        return originalGenericStopIds;
    }

    /**
     * Collect all trip IDs so that new stop times can be inserted (with the appropriate trip ID value) if a pattern
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
        List<String> tripsIdsForPattern = new ArrayList<>();
        while (tripsResults.next()) {
            tripsIdsForPattern.add(tripsResults.getString(1));
        }
        return tripsIdsForPattern;
    }

    /**
     * Get a list of pattern reference ids.
     */
    private static List<String> getPatternReferenceIds(List<GenericStop> genericStops) {
        return genericStops.stream().map(pattern -> pattern.referenceId).collect(Collectors.toList());
    }

    /**
     * Insert blank stop times. This must be called after updating sequences for any stop times following the starting
     * stop sequence to avoid overwriting these other stop times.
     */
    private static void insertBlankStopTimes(
        String tablePrefix,
        List<String> tripIds,
        List<GenericStop> genericStops,
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
            StopTime stopTime = genericStops.get(i).stopTime;
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
     * Check that the stops in the changed region remain in the same order. If not, throw an exception to cancel the
     * transaction.
     */
    private static void verifyInteriorStopsAreUnchanged(
        List<String> originalStopIds,
        List<String> newStopIds,
        int firstDifferentIndex,
        int lastDifferentIndex
    ) {
        for (int i = firstDifferentIndex; i <= (lastDifferentIndex - 1); i++) {
            String newStopId = newStopIds.get(i);
            // Because a stop was inserted at position firstDifferentIndex, all original stop ids are shifted by one.
            String originalStopId = originalStopIds.get(i + 1);
            if (!newStopId.equals(originalStopId)) {
                // If the new stop ID at the given index does not match the original stop ID, the order of at least
                // one stop within the changed region has been changed. This is illegal according to the rule enforcing
                // only a single addition, deletion, or transposition per update.
                throw new IllegalStateException(RECONCILE_STOPS_ERROR_MSG);
            }
        }
    }

    /**
     * Generic stop class use to hold either a pattern stop or pattern location derived data.
     */
    private static class GenericStop {
        public String referenceId;
        // This stopTime object is a template that will be used to build database statements.
        StopTime stopTime;
        public GenericStop(PatternLocation patternLocation) {
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

        public GenericStop(PatternStop patternStop) {
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
