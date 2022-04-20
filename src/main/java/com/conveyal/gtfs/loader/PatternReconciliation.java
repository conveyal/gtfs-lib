package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.model.PatternLocation;
import com.conveyal.gtfs.model.PatternStop;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.util.json.JsonManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Update a trip pattern stops/locations and associated stop times.
 */
public class PatternReconciliation {

    private static final Logger LOG = LoggerFactory.getLogger(PatternReconciliation.class);
    private static final String RECONCILE_STOPS_ERROR_MSG = "Changes to trip pattern stops or locations must be made one at a time if pattern contains at least one trip.";
    private final List<String> originalGenericStopIds = new ArrayList<>();
    private final PreparedStatement getGenericStopIdsStatement;
    private final Connection connection;
    private final String tablePrefix;
    private String patternId;
    private String joinToTrips;
    private List<String> tripsForPattern;
    private List<GenericStop> newGenericStops;
    private List<PatternLocation> patternLocations = new ArrayList<>();
    private List<PatternStop> patternStops = new ArrayList<>();

    /**
     * Enum containing available reconciliation operations.
     */
    private enum ReconciliationOperation {
        ADD_ONE, ADD_MULTIPLE, DELETE, TRANSPOSE, NONE
    }

    /**
     * Enum containing available pattern types.
     */
    public enum PatternType {
        LOCATION, STOP
    }

    public PatternReconciliation(Connection connection, String tablePrefix) throws SQLException {
        this.connection = connection;
        this.tablePrefix = tablePrefix;
        getGenericStopIdsStatement = connection.prepareStatement(
            String.format("select location_id, stop_sequence from %s.pattern_locations pl " +
                    "where pl.pattern_id = ? " +
                    "union " +
                    "select stop_id, stop_sequence from %s.pattern_stops ps " +
                    "where ps.pattern_id = ? " +
                    "order by stop_sequence",
                tablePrefix,
                tablePrefix
            )
        );
    }

    /**
     * Pattern reconciliation requires all new pattern stops and pattern locations as well as the original values of both
     * to correctly update stop times. Because these entities are processed in series, this method is used to accumulate
     * all required values when available. The values are then used by {@link PatternReconciliation#reconcile} _after_
     * all child entities have been processed.
     */
    public void stage(
        ObjectMapper mapper,
        Table subTable,
        ArrayNode subEntities,
        String keyValue
    ) throws SQLException, JsonProcessingException {
        patternId = keyValue;
        if (originalGenericStopIds.isEmpty()) {
            // Retrieve all generic stop ids before they are updated.
            getGenericStopIdsStatement.setString(1, keyValue);
            getGenericStopIdsStatement.setString(2, keyValue);
            LOG.info(getGenericStopIdsStatement.toString());
            ResultSet locationsResults = getGenericStopIdsStatement.executeQuery();
            while (locationsResults.next()) {
                originalGenericStopIds.add(locationsResults.getString(1));
            }
        }
        if (Table.PATTERN_STOP.name.equals(subTable.name)) {
            // Accumulate new pattern stop objects from JSON.
            patternStops = JsonManager.read(mapper, subEntities, PatternStop.class);
        }
        if (Table.PATTERN_LOCATION.name.equals(subTable.name)) {
            // Accumulate new pattern location objects from JSON.
            patternLocations = JsonManager.read(mapper, subEntities, PatternLocation.class);
        }
    }

    /**
     * Reconcile pattern stops and pattern locations.
     */
    public void reconcile() throws SQLException {
        if (patternLocations.isEmpty() && patternStops.isEmpty()) {
            LOG.info("No pattern stops/locations provided. Pattern reconciliation not required.");
            return;
        }
        tripsForPattern = getTripIdsForPatternId();
        if (tripsForPattern.isEmpty()) {
            LOG.info("No associated trips for pattern id {}. Pattern reconciliation not required.", patternId);
            return;
        }
        newGenericStops = getGenericStops();
        ReconciliationOperation reconciliationOperation = getOperation(originalGenericStopIds, newGenericStops);
        if (reconciliationOperation == ReconciliationOperation.NONE) {
            LOG.info("Pattern stops not changed. Pattern reconciliation not required.");
            return;
        }
        // Prepare SQL fragment to filter for all stop times for all trips on a certain pattern.
        joinToTrips = String.format(
            "%s.trips.trip_id = %s.stop_times.trip_id AND %s.trips.pattern_id = '%s'",
            tablePrefix,
            tablePrefix,
            tablePrefix,
            patternId
        );
        reconcilePattern(reconciliationOperation);
    }

    /**
     * Return pattern location matching the provided reference id.
     */
    public PatternLocation getPatternLocation(String referenceId) {
        return patternLocations
            .stream()
            .filter(pl -> pl.location_id.equals(referenceId))
            .findFirst()
            .get();
    }

    /**
     * Return pattern stop matching the provided reference id.
     */
    public PatternStop getPatternStop(String referenceId) {
        return patternStops
            .stream()
            .filter(ps -> ps.stop_id.equals(referenceId))
            .findFirst()
            .get();
    }

    /**
     * Combine pattern locations and pattern stops then order by stop sequence.
     */
    public List<GenericStop> getGenericStops() {
        List<GenericStop> genericStops = patternStops.stream().map(GenericStop::new).collect(Collectors.toList());
        genericStops.addAll(patternLocations.stream().map(GenericStop::new).collect(Collectors.toList()));
        genericStops.sort(Comparator.comparingInt(genericStop -> genericStop.stopTime.stop_sequence));
        return genericStops;
    }

    /**
     * Define the type of update to be applied to stop times. This is done by comparing the size of the original generic
     * stops against the size of the new generic stops.
     */
    private ReconciliationOperation getOperation(List<String> originalGenericStopIds, List<GenericStop> genericStops) {
        int sizeDiff = genericStops.size() - originalGenericStopIds.size();
        if (sizeDiff == 1) {
            return ReconciliationOperation.ADD_ONE;
        } else if (sizeDiff == -1) {
            return ReconciliationOperation.DELETE;
        } else if (sizeDiff == 0) {
            return getFirstTripPatternDifference() == -1
                ? ReconciliationOperation.NONE
                : ReconciliationOperation.TRANSPOSE;
        } else if (sizeDiff > 1) {
            return ReconciliationOperation.ADD_MULTIPLE;
        } else {
            // Any other type of modification is not supported.
            throw new IllegalStateException(RECONCILE_STOPS_ERROR_MSG);
        }
    }

    /**
     * We assume only one stop time has changed, either it's been removed, added or moved. The only other case that is
     * permitted is adding a set of stops/locations to the end of the original list. These conditions are evaluated by
     * simply checking the lengths of the original and new pattern stops/locations (and ensuring that stop IDs remain
     * the same where required). If the change to pattern stops/locations does not satisfy one of these cases, fail the
     * update operation.
     */
    private void reconcilePattern(ReconciliationOperation reconciliationOperation) throws SQLException {
        LOG.info("Reconciling pattern for pattern Id: {}", patternId);
        switch (reconciliationOperation) {
            case ADD_ONE:
                addOneStopToAPattern();
                break;
            case DELETE:
                deleteOneStopFromAPattern();
                break;
            case TRANSPOSE:
                transposeStopInAPattern();
                break;
            case ADD_MULTIPLE:
                addOneOrMoreStopsToEndOfPattern();
                break;
        }
    }

    /**
     * Add a single generic stop to a pattern.
     */
    private void addOneStopToAPattern() throws SQLException {
        // We have an addition; find it.
        int differenceLocation = checkForGenericStopDifference();
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
        insertBlankStopTimes(differenceLocation, 1);
    }

    /**
     * Find and delete one generic stop from within a pattern.
     */
    private void deleteOneStopFromAPattern() throws SQLException {
        // We have a deletion; find it.
        int differenceLocation = checkForOriginalPatternDifference();
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
     * Move one generic stop within a pattern.
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
    private void transposeStopInAPattern() throws SQLException {
        // Find the left bound of the changed region.
        int firstDifferentIndex = getFirstTripPatternDifference();
        if (firstDifferentIndex == -1) {
            // Trip patterns do not differ at all, nothing to do.
            return;
        }
        // Find the right bound of the changed region.
        int lastDifferentIndex = originalGenericStopIds.size() - 1;
        while (originalGenericStopIds.get(lastDifferentIndex).equals(newGenericStops.get(lastDifferentIndex).referenceId)) {
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
        List<String> newReferenceIds = getPatternReferenceIds(newGenericStops);
        // Ensure that only a single stop has been moved (i.e. verify stop IDs inside changed region remain unchanged)
        if (originalGenericStopIds.get(firstDifferentIndex).equals(newGenericStops.get(lastDifferentIndex).referenceId)) {
            // Stop was moved from beginning of changed region to end of changed region (-->)
            from = firstDifferentIndex;
            to = lastDifferentIndex;
            // If sequence is greater than fromIndex and less than or equal to toIndex, decrement.
            arithmeticOperator = "-";
        } else if (newGenericStops.get(firstDifferentIndex).referenceId.equals(originalGenericStopIds.get(lastDifferentIndex))) {
            // Stop was moved from end of changed region to beginning of changed region (<--)
            from = lastDifferentIndex;
            to = firstDifferentIndex;
            // If sequence is less than fromIndex and greater than or equal to toIndex, increment.
            arithmeticOperator = "+";
        } else {
            throw new IllegalStateException("not a simple, single move!");
        }
        verifyInteriorStopsAreUnchanged(newReferenceIds, firstDifferentIndex, lastDifferentIndex);
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
    private void addOneOrMoreStopsToEndOfPattern() throws SQLException {
        // find the left bound of the changed region to check that no stops have changed in between
        int firstDifferentIndex = 0;
        while (
            firstDifferentIndex < originalGenericStopIds.size() &&
                originalGenericStopIds.get(firstDifferentIndex).equals(newGenericStops.get(firstDifferentIndex).referenceId)
        ) {
            firstDifferentIndex++;
        }
        if (firstDifferentIndex != originalGenericStopIds.size())
            throw new IllegalStateException("When adding multiple stops to patterns, new stops must all be at the end");

        // insert a skipped stop for each new element in newStops
        int stopsToInsert = newGenericStops.size() - firstDifferentIndex;
        // FIXME: Should we be inserting blank stop times at all?  Shouldn't these just inherit the arrival times
        // from the pattern stops?
        LOG.info("Adding {} stop times to existing {} stop times. Starting at {}",
            stopsToInsert,
            originalGenericStopIds.size(),
            firstDifferentIndex
        );
        insertBlankStopTimes(firstDifferentIndex, stopsToInsert);
    }

    /**
     * Check if there is a difference between the original and new trip patterns. Return the first difference or
     * -1 if there is no difference.
     */
    private int getFirstTripPatternDifference() {
        int firstDifferentIndex = 0;
        while (originalGenericStopIds.get(firstDifferentIndex).equals(newGenericStops.get(firstDifferentIndex).referenceId)) {
            firstDifferentIndex++;
            if (firstDifferentIndex == originalGenericStopIds.size())
                // Trip patterns do not differ at all, nothing to do.
                return -1;
        }
        return firstDifferentIndex;
    }

    /**
     * Check the new generic stop ids for differences against the original generic stop ids. If only one change has
     * been made (expected behaviour) return the index with the difference. If more than one difference is found
     * throw an exception.
     */
    private int checkForGenericStopDifference() {
        int differenceLocation = -1;
        for (int i = 0; i < newGenericStops.size(); i++) {
            if (differenceLocation != -1) {
                if (
                    i < originalGenericStopIds.size() &&
                        !originalGenericStopIds.get(i).equals(newGenericStops.get(i + 1).referenceId)
                ) {
                    // The addition has already been found and there's another difference, which we weren't expecting
                    throw new IllegalStateException("Multiple differences found when trying to detect stop addition");
                }
            } else if (
                i == newGenericStops.size() - 1 ||
                    !originalGenericStopIds.get(i).equals(newGenericStops.get(i).referenceId)
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
    private int checkForOriginalPatternDifference() {
        int differenceLocation = -1;
        for (int i = 0; i < originalGenericStopIds.size(); i++) {
            if (differenceLocation != -1) {
                if (!originalGenericStopIds.get(i).equals(newGenericStops.get(i - 1).referenceId)) {
                    // There is another difference, which we were not expecting.
                    throw new IllegalStateException("Multiple differences found when trying to detect stop removal.");
                }
            } else if (
                i == originalGenericStopIds.size() - 1 ||
                    !originalGenericStopIds.get(i).equals(newGenericStops.get(i).referenceId)
            ) {
                // We've reached the end and the only difference is length (so the last stop is the different one)
                // or we've found the difference.
                differenceLocation = i;
            }
        }
        return differenceLocation;
    }

    /**
     * Collect all trip IDs so that new stop times can be inserted (with the appropriate trip ID value) if a pattern
     * is added.
     */
    private List<String> getTripIdsForPatternId() throws SQLException {
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
    private List<String> getPatternReferenceIds(List<GenericStop> genericStops) {
        return genericStops.stream().map(pattern -> pattern.referenceId).collect(Collectors.toList());
    }

    /**
     * Insert blank stop times. This must be called after updating sequences for any stop times following the starting
     * stop sequence to avoid overwriting these other stop times.
     */
    private void insertBlankStopTimes(
        int startingStopSequence,
        int stopTimesToAdd
    ) throws SQLException {
        if (tripsForPattern.isEmpty()) {
            // There is no need to insert blank stop times if there are no trips for the pattern.
            return;
        }
        String insertSql = Table.STOP_TIMES.generateInsertSql(tablePrefix, true);
        PreparedStatement insertStatement = connection.prepareStatement(insertSql);
        int totalRowsUpdated = 0;
        // Create a new stop time for each sequence value (times each trip ID) that needs to be inserted.
        for (int i = startingStopSequence; i < stopTimesToAdd + startingStopSequence; i++) {
            StopTime stopTime = newGenericStops.get(i).stopTime;
            stopTime.stop_sequence = i;
            // Update stop time with each trip ID and add to batch.
            for (String tripId : tripsForPattern) {
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
    private void verifyInteriorStopsAreUnchanged(
        List<String> newStopIds,
        int firstDifferentIndex,
        int lastDifferentIndex
    ) {
        for (int i = firstDifferentIndex; i <= (lastDifferentIndex - 1); i++) {
            String newStopId = newStopIds.get(i);
            // Because a stop was inserted at position firstDifferentIndex, all original stop ids are shifted by one.
            String originalStopId = originalGenericStopIds.get(i + 1);
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
    static class GenericStop {
        public String referenceId;
        // This stopTime object is a template that will be used to build database statements.
        StopTime stopTime;
        PatternType patternType;

        public GenericStop(PatternLocation patternLocation) {
            patternType = PatternType.LOCATION;
            referenceId = patternLocation.location_id;
            stopTime = new StopTime();
            stopTime.stop_id = patternLocation.location_id;
            stopTime.stop_sequence = patternLocation.stop_sequence;
            stopTime.drop_off_type = patternLocation.drop_off_type;
            stopTime.pickup_type = patternLocation.pickup_type;
            stopTime.timepoint = patternLocation.timepoint;
            stopTime.continuous_drop_off = patternLocation.continuous_drop_off;
            stopTime.continuous_pickup = patternLocation.continuous_pickup;
            stopTime.pickup_booking_rule_id = patternLocation.pickup_booking_rule_id;
            stopTime.drop_off_booking_rule_id = patternLocation.drop_off_booking_rule_id;
            stopTime.mean_duration_offset = patternLocation.mean_duration_offset;
            stopTime.mean_duration_factor = patternLocation.mean_duration_factor;
            stopTime.safe_duration_offset = patternLocation.safe_duration_offset;
            stopTime.safe_duration_factor = patternLocation.safe_duration_factor;
        }

        public GenericStop(PatternStop patternStop) {
            patternType = PatternType.STOP;
            referenceId = patternStop.stop_id;
            stopTime = new StopTime();
            stopTime.stop_id = patternStop.stop_id;
            stopTime.stop_sequence = patternStop.stop_sequence;
            stopTime.drop_off_type = patternStop.drop_off_type;
            stopTime.pickup_type = patternStop.pickup_type;
            stopTime.timepoint = patternStop.timepoint;
            stopTime.shape_dist_traveled = patternStop.shape_dist_traveled;
            stopTime.continuous_drop_off = patternStop.continuous_drop_off;
            stopTime.continuous_pickup = patternStop.continuous_pickup;
        }
    }
}
