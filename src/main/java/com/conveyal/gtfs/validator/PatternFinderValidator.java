package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.PatternFinder;
import com.conveyal.gtfs.TripPatternKey;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.BatchTracker;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.loader.PatternReconciliation;
import com.conveyal.gtfs.loader.Requirement;
import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.model.Area;
import com.conveyal.gtfs.model.Location;
import com.conveyal.gtfs.model.Pattern;
import com.conveyal.gtfs.model.PatternLocation;
import com.conveyal.gtfs.model.PatternStop;
import com.conveyal.gtfs.model.PatternStopArea;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopArea;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.conveyal.gtfs.loader.JdbcGtfsLoader.copyFromFile;
import static com.conveyal.gtfs.model.Entity.INT_MISSING;

/**
 * Groups trips together into "patterns" that share the same sequence of stops.
 * This is not a normal validator in the sense that it does not check for bad data.
 * It's taking advantage of the fact that we're already iterating over the trips one by one to build up the patterns.
 */
public class PatternFinderValidator extends TripValidator {

    private static final Logger LOG = LoggerFactory.getLogger(PatternFinderValidator.class);

    PatternFinder patternFinder;
    private File tempPatternForTripsTextFile;
    private PrintStream patternForTripsFileStream;
    private String tempPatternForTripsTable;

    public PatternFinderValidator(Feed feed, SQLErrorStorage errorStorage) {
        super(feed, errorStorage);
        patternFinder = new PatternFinder();
    }

    @Override
    public void validateTrip(
        Trip trip,
        Route route,
        List<StopTime> stopTimes,
        List<Stop> stops,
        List<Location> locations,
        List<StopArea> stopAreas
    ) {
        // As we hit each trip, accumulate them into the wrapped PatternFinder object.
        patternFinder.processTrip(trip, stopTimes);
    }

    /**
     * Store patterns and pattern stops in the database. Also, update the trips table with a pattern_id column.
     */
    @Override
    public void complete(ValidationResult validationResult) {
        LOG.info("Finding patterns...");
        // FIXME: There may be a better way to handle getting the full list of stops
        Map<String, Stop> stopById = new HashMap<>();
        Map<String, Location> locationById = new HashMap<>();
        Map<String, StopArea> stopAreaById = new HashMap<>();
        Map<String, Area> areaById = new HashMap<>();
        for (Stop stop : feed.stops) {
            stopById.put(stop.stop_id, stop);
        }
        for (Location location : feed.locations) {
            locationById.put(location.location_id, location);
        }
        for (StopArea stopArea : feed.stopAreas) {
            stopAreaById.put(stopArea.area_id, stopArea);
        }
        for (Area area : feed.areas) {
            areaById.put(area.area_id, area);
        }
        // FIXME In the editor we need patterns to exist separately from and before trips themselves, so me make another table.
        Map<TripPatternKey, Pattern> patterns = patternFinder.createPatternObjects(stopById, locationById, stopAreaById, areaById, errorStorage);
        Connection connection = null;
        PreparedStatement insertPatternStatement = null;
        PreparedStatement insertPatternStopStatement = null;
        PreparedStatement insertPatternLocationStatement = null;
        PreparedStatement insertPatternStopAreaStatement = null;
        try {
            // TODO this assumes gtfs-lib is using an SQL database and not a MapDB.
            //   Maybe we should just create patterns in a separate step, but that would mean iterating over the
            //   stop_times twice.
            LOG.info("Creating pattern and pattern stops and pattern locations tables.");
            connection = feed.getConnection();
            Statement statement = connection.createStatement();
            String tripsTableName = feed.tablePrefix + "trips";
            String patternsTableName = feed.tablePrefix + "patterns";
            String patternStopsTableName = feed.tablePrefix + "pattern_stops";
            String patternLocationsTableName = feed.tablePrefix + "pattern_locations";
            String patternStopAreasTableName = feed.tablePrefix + "pattern_stop_areas";
            statement.execute(String.format("alter table %s add column pattern_id varchar", tripsTableName));
            // FIXME: Here we're creating a pattern table that has an integer ID field (similar to the other GTFS tables)
            //   AND a varchar pattern_id with essentially the same value cast to a string. Perhaps the pattern ID should
            //   be a UUID or something, just to better distinguish it from the int ID?
            Table patternsTable = new Table(patternsTableName, Pattern.class, Requirement.EDITOR, Table.PATTERNS.fields);
            Table patternStopsTable = new Table(patternStopsTableName, PatternStop.class, Requirement.EDITOR,
                Table.PATTERN_STOP.fields);
            Table patternLocationsTable = new Table(patternLocationsTableName, PatternLocation.class, Requirement.EDITOR,
                Table.PATTERN_LOCATION.fields);
            Table patternStopAreasTable = new Table(
                patternStopAreasTableName,
                PatternStopArea.class,
                Requirement.EDITOR,
                Table.PATTERN_STOP_AREA.fields
            );

            // Create pattern tables, each with serial ID fields.
            patternsTable.createSqlTable(connection, null, true);
            patternStopsTable.createSqlTable(connection, null, true);
            patternLocationsTable.createSqlTable(connection, null, true);
            patternStopAreasTable.createSqlTable(connection, null, true);

            // Generate prepared statements for inserts.
            insertPatternStatement = connection.prepareStatement(patternsTable.generateInsertSql(true));
            insertPatternStopStatement = connection.prepareStatement(patternStopsTable.generateInsertSql(true));
            insertPatternLocationStatement = connection.prepareStatement(patternLocationsTable.generateInsertSql(true));
            insertPatternStopAreaStatement = connection.prepareStatement(patternStopAreasTable.generateInsertSql(true));

            // Generate batch trackers for inserts.
            BatchTracker patternTracker = new BatchTracker("pattern", insertPatternStatement);
            BatchTracker patternStopTracker = new BatchTracker("pattern stop", insertPatternStopStatement);
            BatchTracker patternLocationTracker = new BatchTracker("pattern location", insertPatternLocationStatement);
            BatchTracker patternStopAreaTracker = new BatchTracker("pattern stop area", insertPatternStopAreaStatement);

            int currentPatternIndex = 0;
            LOG.info("Storing patterns, pattern stops and pattern locations");
            // If using Postgres, load pattern to trips mapping into temp table for quick updating.
            boolean postgresText = (connection.getMetaData().getDatabaseProductName().equals("PostgreSQL"));
            if (postgresText) {
                // NOTE: temp table name must NOT be prefixed with schema because temp tables are prefixed with their own
                // connection-unique schema.
                tempPatternForTripsTable = "pattern_for_trips";
                tempPatternForTripsTextFile = File.createTempFile(tempPatternForTripsTable, "text");
                LOG.info("Loading via temporary text file at {}", tempPatternForTripsTextFile.getAbsolutePath());
                // Create temp table for updating trips with pattern IDs to be dropped at the end of the transaction.
                String createTempSql = String.format("create temp table %s(trip_id varchar, pattern_id varchar) on commit drop", tempPatternForTripsTable);
                LOG.info(createTempSql);
                statement.execute(createTempSql);
                patternForTripsFileStream = new PrintStream(new BufferedOutputStream(new FileOutputStream(tempPatternForTripsTextFile)));
            }
            for (Map.Entry<TripPatternKey, Pattern> entry : patterns.entrySet()) {
                Pattern pattern = entry.getValue();
                LOG.debug("Batching pattern {}", pattern.pattern_id);
                TripPatternKey key = entry.getKey();
                pattern.setStatementParameters(insertPatternStatement, true);
                patternTracker.addBatch();
                // Determine departure times based on the stop type.
                List<Integer> previousDepartureTimes = calculatePreviousDepartureTimes(key, locationById, stopAreaById);
                // Construct pattern stops based on values in trip pattern key.
                for (int stopSequence = 0; stopSequence < key.stops.size(); stopSequence++) {
                    String stopOrLocationIdOrStopAreaId = key.stops.get(stopSequence);
                    boolean prevIsFlexStop = stopSequence > 0 && isFlexStop(locationById, stopAreaById, key.stops.get(stopSequence - 1));
                    int lastValidDepartureTime = previousDepartureTimes.get(stopSequence);
                    if (stopById.containsKey(stopOrLocationIdOrStopAreaId)) {
                        insertPatternType(
                            stopSequence,
                            key,
                            lastValidDepartureTime,
                            pattern.pattern_id,
                            insertPatternStopStatement,
                            patternStopTracker,
                            stopOrLocationIdOrStopAreaId,
                            PatternReconciliation.PatternType.STOP,
                            prevIsFlexStop
                        );
                    } else if (locationById.containsKey(stopOrLocationIdOrStopAreaId)) {
                        insertPatternType(
                            stopSequence,
                            key,
                            lastValidDepartureTime,
                            pattern.pattern_id,
                            insertPatternLocationStatement,
                            patternLocationTracker,
                            stopOrLocationIdOrStopAreaId,
                            PatternReconciliation.PatternType.LOCATION,
                            prevIsFlexStop
                        );
                    } else if (stopAreaById.containsKey(stopOrLocationIdOrStopAreaId)) {
                        insertPatternType(
                            stopSequence,
                            key,
                            lastValidDepartureTime,
                            pattern.pattern_id,
                            insertPatternStopAreaStatement,
                            patternStopAreaTracker,
                            stopOrLocationIdOrStopAreaId,
                            PatternReconciliation.PatternType.STOP_AREA,
                            prevIsFlexStop
                        );
                    }
                }
                // Finally, update all trips on this pattern to reference this pattern's ID.
                String questionMarks = String.join(", ", Collections.nCopies(pattern.associatedTrips.size(), "?"));
                PreparedStatement updateTripStatement = connection.prepareStatement(
                    String.format("update %s set pattern_id = ? where trip_id in (%s)", tripsTableName, questionMarks));
                int oneBasedIndex = 1;
                updateTripStatement.setString(oneBasedIndex++, pattern.pattern_id);
                // Prepare each trip in pattern to update trips table.
                for (String tripId : pattern.associatedTrips) {
                    if (postgresText) {
                        // Add line to temp csv file if using postgres.
                        // No need to worry about null trip IDs because the trips have already been processed.
                        String[] strings = new String[]{tripId, pattern.pattern_id};
                        // Print a new line in the standard postgres text format:
                        // https://www.postgresql.org/docs/9.1/static/sql-copy.html#AEN64380
                        patternForTripsFileStream.println(String.join("\t", strings));
                    } else {
                        // Otherwise, set statement parameter.
                        updateTripStatement.setString(oneBasedIndex++, tripId);
                    }
                }
                if (!postgresText) {
                    // Execute trip update statement if not using temp text file.
                    LOG.info("Updating {} trips with pattern ID {} (%d/%d)", pattern.associatedTrips.size(), pattern.pattern_id, currentPatternIndex, patterns.size());
                    updateTripStatement.executeUpdate();
                }
                currentPatternIndex += 1;
            }
            // Send any remaining prepared statement calls to the database backend.
            patternTracker.executeRemaining();
            patternStopTracker.executeRemaining();
            patternLocationTracker.executeRemaining();
            patternStopAreaTracker.executeRemaining();
            LOG.info("Done storing patterns and pattern stops and pattern locations.");
            if (postgresText) {
                // Finally, copy the pattern for trips text file into a table, create an index on trip IDs, and update
                // the trips table.
                LOG.info("Updating trips with pattern IDs");
                patternForTripsFileStream.close();
                // Copy file contents into temp pattern for trips table.
                copyFromFile(connection, tempPatternForTripsTextFile, tempPatternForTripsTable);
                // Before updating the trips with pattern IDs, index the table on trip_id.
                String patternForTripsIndexSql = String.format("create index temp_trips_pattern_id_idx on %s (trip_id)", tempPatternForTripsTable);
                LOG.info(patternForTripsIndexSql);
                statement.execute(patternForTripsIndexSql);
                // Finally, execute the update statement.
                String updateTripsSql = String.format("update %s set pattern_id = %s.pattern_id from %s where %s.trip_id = %s.trip_id", tripsTableName, tempPatternForTripsTable, tempPatternForTripsTable, tripsTableName, tempPatternForTripsTable);
                LOG.info(updateTripsSql);
                statement.executeUpdate(updateTripsSql);
                // Delete temp file. Temp table will be dropped after the transaction is committed.
                tempPatternForTripsTextFile.delete();
                LOG.info("Updating trips complete");
            }
            LOG.info("Creating index on patterns");
            statement.executeUpdate(String.format("alter table %s add primary key (pattern_id)", patternsTableName));
            LOG.info("Creating index on pattern stops");
            statement.executeUpdate(String.format("alter table %s add primary key (pattern_id, stop_sequence)", patternStopsTableName));
            LOG.info("Creating index on pattern locations");
            statement.executeUpdate(String.format("alter table %s add primary key (pattern_id, stop_sequence)", patternLocationsTableName));
            // Index new pattern_id column on trips. The other tables are already indexed because they have primary keys.
            LOG.info("Indexing trips on pattern id.");
            statement.execute(String.format("create index trips_pattern_id_idx on %s (pattern_id)", tripsTableName));
            LOG.info("Done indexing.");
            connection.commit();
        } catch (SQLException | IOException e) {
            // Rollback transaction if failure occurs on creating patterns.
            DbUtils.rollbackAndCloseQuietly(connection);
            // This exception will be stored as a validator failure.
            throw new RuntimeException(e);
        } finally {
            // Close connection and insert statements quietly.
            if (connection != null) DbUtils.closeQuietly(connection);
            if (insertPatternStatement != null) DbUtils.closeQuietly(insertPatternStatement);
            if (insertPatternStopStatement != null) DbUtils.closeQuietly(insertPatternStopStatement);
            if (insertPatternLocationStatement != null) DbUtils.closeQuietly(insertPatternLocationStatement);
            if (insertPatternStopAreaStatement != null) DbUtils.closeQuietly(insertPatternStopAreaStatement);
        }

    }

    /**
     * Determine if the provided stop id is a flex stop (location or stop area).
     */
    private boolean isFlexStop(
        Map<String, Location> locationById,
        Map<String, StopArea> stopAreaById,
        String stopId) {
        return locationById.containsKey(stopId) || stopAreaById.containsKey(stopId);
    }

    /**
     * Calculate previous departure times, needed for all patterns. This is done by defining the 'last valid departure
     * time' for all stops. The previous departure time for the first stop will always be zero.
     */
    public List<Integer> calculatePreviousDepartureTimes(
        TripPatternKey key,
        Map<String, Location> locationById,
        Map<String, StopArea> stopAreaById
    ) {
        List<Integer> previousDepartureTimes = new ArrayList<>();
        // Determine initial departure time based on the stop type.
        int lastValidDepartureTime = isFlexStop(locationById, stopAreaById, key.stops.get(0))
            ? key.end_pickup_drop_off_window.get(0)
            : key.departureTimes.get(0);
        // Set the previous departure time for the first stop, which will always be zero.
        previousDepartureTimes.add(0);
        // Construct pattern stops based on values in trip pattern key.
        for (int stopSequence = 1; stopSequence < key.stops.size(); stopSequence++) {
            boolean prevIsFlexStop = isFlexStop(locationById, stopAreaById, key.stops.get(stopSequence - 1));
            boolean currentIsFlexStop = isFlexStop(locationById, stopAreaById, key.stops.get(stopSequence));
            // Set travel time for all stops except the first.
            if (prevIsFlexStop && currentIsFlexStop) {
                // Previous and current are flex stops. There is no departure time between flex stops.
                lastValidDepartureTime = 0;
            } else {
                int prevDepartureStop = prevIsFlexStop
                    ? key.end_pickup_drop_off_window.get(stopSequence - 1)
                    : key.departureTimes.get(stopSequence - 1);
                if (prevDepartureStop > lastValidDepartureTime) {
                    // Update the last valid departure if the previous departure is after this. Otherwise, continue to
                    // use the most recent valid departure.
                    lastValidDepartureTime = prevDepartureStop;
                }
            }
            previousDepartureTimes.add(lastValidDepartureTime);
        }
        return previousDepartureTimes;
    }

    /**
     * Insert pattern types. This covers pattern stops, locations and stop areas.
     */
    private void insertPatternType(
        int stopSequence,
        TripPatternKey tripPattern,
        int lastValidDeparture,
        String patternId,
        PreparedStatement statement,
        BatchTracker batchTracker,
        String patternTypeId,
        PatternReconciliation.PatternType patternType,
        boolean prevIsFlexStop
    ) throws SQLException {
        int travelTime = 0;
        if (patternType == PatternReconciliation.PatternType.STOP) {
            travelTime = getTravelTime(
                travelTime,
                stopSequence,
                tripPattern.arrivalTimes.get(stopSequence),
                lastValidDeparture);
        } else if (!prevIsFlexStop) {
            // If the previous stop is not flex, calculate travel time. If the previous stop is flex the travel time will
            // be zero.
            travelTime = getTravelTime(
                travelTime,
                stopSequence,
                tripPattern.start_pickup_drop_off_window.get(stopSequence),
                lastValidDeparture);
        }
        int timeInLocation = (patternType == PatternReconciliation.PatternType.STOP)
            ? getTimeInLocation(
                tripPattern.arrivalTimes.get(stopSequence),
                tripPattern.departureTimes.get(stopSequence))
            : getTimeInLocation(
                tripPattern.start_pickup_drop_off_window.get(stopSequence),
                tripPattern.end_pickup_drop_off_window.get(stopSequence));

        if (patternType == PatternReconciliation.PatternType.STOP) {
            PatternStop patternStop = new PatternStop();
            patternStop.pattern_id = patternId;
            patternStop.stop_sequence = stopSequence;
            patternStop.stop_id = patternTypeId;
            patternStop.stop_headsign = tripPattern.stopHeadsigns.get(stopSequence);
            patternStop.default_travel_time = travelTime;
            patternStop.default_dwell_time = timeInLocation;
            patternStop.drop_off_type = tripPattern.dropoffTypes.get(stopSequence);
            patternStop.pickup_type = tripPattern.pickupTypes.get(stopSequence);
            patternStop.shape_dist_traveled = tripPattern.shapeDistances.get(stopSequence);
            patternStop.timepoint = tripPattern.timepoints.get(stopSequence);
            patternStop.continuous_pickup = tripPattern.continuous_pickup.get(stopSequence);
            patternStop.continuous_drop_off = tripPattern.continuous_drop_off.get(stopSequence);
            patternStop.pickup_booking_rule_id = tripPattern.pickup_booking_rule_id.get(stopSequence);
            patternStop.drop_off_booking_rule_id = tripPattern.drop_off_booking_rule_id.get(stopSequence);
            patternStop.setStatementParameters(statement, true);
        } else if (patternType == PatternReconciliation.PatternType.LOCATION) {
            PatternLocation patternLocation = new PatternLocation();
            patternLocation.pattern_id = patternId;
            patternLocation.stop_sequence = stopSequence;
            patternLocation.location_id = patternTypeId;
            patternLocation.drop_off_type = tripPattern.dropoffTypes.get(stopSequence);
            patternLocation.pickup_type = tripPattern.pickupTypes.get(stopSequence);
            patternLocation.timepoint = tripPattern.timepoints.get(stopSequence);
            patternLocation.continuous_pickup = tripPattern.continuous_pickup.get(stopSequence);
            patternLocation.continuous_drop_off = tripPattern.continuous_drop_off.get(stopSequence);
            patternLocation.pickup_booking_rule_id = tripPattern.pickup_booking_rule_id.get(stopSequence);
            patternLocation.drop_off_booking_rule_id = tripPattern.drop_off_booking_rule_id.get(stopSequence);
            patternLocation.flex_default_travel_time = travelTime;
            patternLocation.flex_default_zone_time = timeInLocation;
            patternLocation.mean_duration_factor = tripPattern.mean_duration_factor.get(stopSequence);
            patternLocation.mean_duration_offset = tripPattern.mean_duration_offset.get(stopSequence);
            patternLocation.safe_duration_factor = tripPattern.safe_duration_factor.get(stopSequence);
            patternLocation.safe_duration_offset = tripPattern.safe_duration_offset.get(stopSequence);
            patternLocation.setStatementParameters(statement, true);
        } else if (patternType == PatternReconciliation.PatternType.STOP_AREA) {
            PatternStopArea patternStopArea = new PatternStopArea();
            patternStopArea.pattern_id = patternId;
            patternStopArea.stop_sequence = stopSequence;
            patternStopArea.area_id = patternTypeId;
            patternStopArea.drop_off_type = tripPattern.dropoffTypes.get(stopSequence);
            patternStopArea.pickup_type = tripPattern.pickupTypes.get(stopSequence);
            patternStopArea.timepoint = tripPattern.timepoints.get(stopSequence);
            patternStopArea.continuous_pickup = tripPattern.continuous_pickup.get(stopSequence);
            patternStopArea.continuous_drop_off = tripPattern.continuous_drop_off.get(stopSequence);
            patternStopArea.pickup_booking_rule_id = tripPattern.pickup_booking_rule_id.get(stopSequence);
            patternStopArea.drop_off_booking_rule_id = tripPattern.drop_off_booking_rule_id.get(stopSequence);
            patternStopArea.flex_default_travel_time = travelTime;
            patternStopArea.flex_default_zone_time = timeInLocation;
            patternStopArea.mean_duration_factor = tripPattern.mean_duration_factor.get(stopSequence);
            patternStopArea.mean_duration_offset = tripPattern.mean_duration_offset.get(stopSequence);
            patternStopArea.safe_duration_factor = tripPattern.safe_duration_factor.get(stopSequence);
            patternStopArea.safe_duration_offset = tripPattern.safe_duration_offset.get(stopSequence);
            patternStopArea.setStatementParameters(statement, true);
        }
        batchTracker.addBatch();
    }

    /**
     * Get the travel time from previous to current stop for pattern stops or travel time within a flex location or
     * stop area.
     */
    private int getTravelTime(int travelTime, int stopSequence, int pickupStart, int lastValidDeparture) {
        if (stopSequence > 0) {
            travelTime = pickupStart == INT_MISSING || lastValidDeparture == INT_MISSING
                ? INT_MISSING
                : pickupStart - lastValidDeparture;
        }
        return travelTime;
    }

    /**
     * Get dwell time for pattern stops or time within a flex location or stop area.
     */
    private int getTimeInLocation(int pickupStart, int pickupEnd) {
        return pickupStart == INT_MISSING || pickupEnd == INT_MISSING
            ? INT_MISSING
            : pickupEnd - pickupStart;
    }
}
