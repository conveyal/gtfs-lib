package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.PatternFinder;
import com.conveyal.gtfs.TripPatternKey;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.BatchTracker;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.loader.Requirement;
import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.model.Location;
import com.conveyal.gtfs.model.LocationGroup;
import com.conveyal.gtfs.model.Pattern;
import com.conveyal.gtfs.model.PatternLocation;
import com.conveyal.gtfs.model.PatternLocationGroup;
import com.conveyal.gtfs.model.PatternStop;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.Stop;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.conveyal.gtfs.loader.JdbcGtfsLoader.copyFromFile;
import static com.conveyal.gtfs.model.Entity.INT_MISSING;
import static com.conveyal.gtfs.model.Entity.setDoubleParameter;
import static com.conveyal.gtfs.model.Entity.setIntParameter;

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
        List<LocationGroup> locationGroups
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
        Map<String, LocationGroup> locationGroupById = new HashMap<>();
        for (Stop stop : feed.stops) {
            stopById.put(stop.stop_id, stop);
        }
        for (Location location : feed.locations) {
            locationById.put(location.location_id, location);
        }
        for (LocationGroup locationGroup : feed.locationGroups) {
            locationGroupById.put(locationGroup.location_group_id, locationGroup);
        }
        // FIXME In the editor we need patterns to exist separately from and before trips themselves, so me make another table.
        Map<TripPatternKey, Pattern> patterns = patternFinder.createPatternObjects(stopById, locationById, errorStorage);
        Connection connection = null;
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
            String patternLocationGroupsTableName = feed.tablePrefix + "pattern_location_groups";
            statement.execute(String.format("alter table %s add column pattern_id varchar", tripsTableName));
            // FIXME: Here we're creating a pattern table that has an integer ID field (similar to the other GTFS tables)
            //   AND a varchar pattern_id with essentially the same value cast to a string. Perhaps the pattern ID should
            //   be a UUID or something, just to better distinguish it from the int ID?
            Table patternsTable = new Table(patternsTableName, Pattern.class, Requirement.EDITOR, Table.PATTERNS.fields);
            Table patternStopsTable = new Table(patternStopsTableName, PatternStop.class, Requirement.EDITOR,
                    Table.PATTERN_STOP.fields);
            Table patternLocationsTable = new Table(patternLocationsTableName, PatternLocation.class, Requirement.EDITOR,
                    Table.PATTERN_LOCATION.fields);
            Table patternLocationGroupsTable = new Table(patternLocationGroupsTableName, PatternLocationGroup.class, Requirement.EDITOR,
                    Table.PATTERN_LOCATION_GROUP.fields);

            // Create pattern tables, each with serial ID fields.
            patternsTable.createSqlTable(connection, null, true);
            patternStopsTable.createSqlTable(connection, null, true);
            patternLocationsTable.createSqlTable(connection, null, true);
            patternLocationGroupsTable.createSqlTable(connection, null, true);

            // Generate prepared statements for inserts.
            PreparedStatement insertPatternStatement = connection.prepareStatement(patternsTable.generateInsertSql(true));
            PreparedStatement insertPatternStopStatement = connection.prepareStatement(patternStopsTable.generateInsertSql(true));
            PreparedStatement insertPatternLocationStatement = connection.prepareStatement(patternLocationsTable.generateInsertSql(true));
            PreparedStatement insertPatternLocationGroupStatement = connection.prepareStatement(patternLocationGroupsTable.generateInsertSql(true));

            // Generate batch trackers for inserts.
            BatchTracker patternTracker = new BatchTracker("pattern", insertPatternStatement);
            BatchTracker patternStopTracker = new BatchTracker("pattern stop", insertPatternStopStatement);
            BatchTracker patternLocationTracker = new BatchTracker("pattern location", insertPatternLocationStatement);
            BatchTracker patternLocationGroupTracker = new BatchTracker("pattern location group", insertPatternLocationGroupStatement);

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
                // Construct pattern stops based on values in trip pattern key.
                // FIXME: Use pattern stops table here?
                int lastValidDeparture = key.departureTimes.get(0);
                for (int i = 0; i < key.stops.size(); i++) {
                    String stopOrLocationIdOrLocationGroupId = key.stops.get(i);
                    int travelTime = 0;
                    // Calculate previous departure time, needed for both pattern location and pattern stop
                    int prevDeparture = INT_MISSING;
                    if (i > 0) {
                        int prevDepartureStop = key.departureTimes.get(i - 1);
                        int prevDepartureFlex = key.end_pickup_dropoff_window.get(i - 1);
                        if (prevDepartureStop != INT_MISSING) {
                            prevDeparture = prevDepartureStop;
                        }
                        // Only use the flex departure if it is after the stop departure.
                        // FLEX TODO: Create a unit test to test this!
                        if (prevDepartureFlex > prevDepartureStop) {
                            prevDeparture = prevDepartureFlex;
                        }

                        // Set travel time for all stops except the first.
                        if (prevDeparture != INT_MISSING) {
                            // Update the previous departure if it's not missing. Otherwise, base travel time based on the
                            // most recent valid departure.
                            lastValidDeparture = prevDeparture;
                        }
                    }
                    if (stopById.containsKey(stopOrLocationIdOrLocationGroupId)) {
                        int arrival = key.arrivalTimes.get(i);
                        if (i > 0) {
                            travelTime = arrival == INT_MISSING || lastValidDeparture == INT_MISSING
                                    ? INT_MISSING
                                    : arrival - lastValidDeparture;
                        }
                        int departure = key.departureTimes.get(i);
                        int dwellTime = arrival == INT_MISSING || departure == INT_MISSING
                                ? INT_MISSING
                                : departure - arrival;

                        insertPatternStopStatement.setString(1, pattern.pattern_id);
                        // Stop sequence is zero-based.
                        setIntParameter(insertPatternStopStatement, 2, i);
                        insertPatternStopStatement.setString(3, stopOrLocationIdOrLocationGroupId);
                        setIntParameter(insertPatternStopStatement, 4, travelTime);
                        setIntParameter(insertPatternStopStatement, 5, dwellTime);
                        setIntParameter(insertPatternStopStatement, 6, key.dropoffTypes.get(i));
                        setIntParameter(insertPatternStopStatement, 7, key.pickupTypes.get(i));
                        setDoubleParameter(insertPatternStopStatement, 8, key.shapeDistances.get(i));
                        setIntParameter(insertPatternStopStatement, 9, key.timepoints.get(i));
                        setIntParameter(insertPatternStopStatement, 10, key.continuous_pickup.get(i));
                        setIntParameter(insertPatternStopStatement, 11, key.continuous_drop_off.get(i));
                        insertPatternStopStatement.setString(12, key.pickup_booking_rule_id.get(i));
                        insertPatternStopStatement.setString(13, key.drop_off_booking_rule_id.get(i));
                        patternStopTracker.addBatch();
                    } else if (locationById.containsKey(stopOrLocationIdOrLocationGroupId)) {
                        addLocationOrLocationGroup(
                            i,
                            key,
                            lastValidDeparture,
                            travelTime,
                            pattern.pattern_id,
                            insertPatternLocationStatement,
                            patternLocationTracker,
                            stopOrLocationIdOrLocationGroupId
                        );
                    } else if (locationGroupById.containsKey(stopOrLocationIdOrLocationGroupId)) {
                        addLocationOrLocationGroup(
                            i,
                            key,
                            lastValidDeparture,
                            travelTime,
                            pattern.pattern_id,
                            insertPatternLocationGroupStatement,
                            patternLocationGroupTracker,
                            stopOrLocationIdOrLocationGroupId
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
            patternLocationGroupTracker.executeRemaining();
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
            // Close transaction finally.
            if (connection != null) DbUtils.closeQuietly(connection);
        }

    }

    /**
     * Insert either a location or location group depending on the value used for the stop id in stops.
     */
    private void addLocationOrLocationGroup(
        int i,
        TripPatternKey key,
        int lastValidDeparture,
        int travelTime,
        String patternId,
        PreparedStatement insertPreparedStatement,
        BatchTracker batchTracker,
        String locationIdOrLocationGroupId
    ) throws SQLException {
        int pickupStart = key.start_pickup_dropoff_window.get(i);
        if (i > 0) {
            travelTime = pickupStart == INT_MISSING || lastValidDeparture == INT_MISSING
                ? INT_MISSING
                : pickupStart - lastValidDeparture;
        }
        int pickupEnd = key.end_pickup_dropoff_window.get(i);
        int timeInLocation = pickupStart == INT_MISSING || pickupEnd == INT_MISSING
            ? INT_MISSING
            : pickupEnd - pickupStart;

        insertPreparedStatement.setString(1, patternId);
        // Stop sequence is zero-based.
        setIntParameter(insertPreparedStatement, 2, i);
        insertPreparedStatement.setString(3, locationIdOrLocationGroupId);
        setIntParameter(insertPreparedStatement, 4, key.pickupTypes.get(i));
        setIntParameter(insertPreparedStatement, 5, key.dropoffTypes.get(i));
        setIntParameter(insertPreparedStatement, 6, key.timepoints.get(i));
        setIntParameter(insertPreparedStatement, 7, key.continuous_pickup.get(i));
        setIntParameter(insertPreparedStatement, 8, key.continuous_drop_off.get(i));
        insertPreparedStatement.setString(9, key.pickup_booking_rule_id.get(i));
        insertPreparedStatement.setString(10, key.drop_off_booking_rule_id.get(i));

        // the derived fields
        setIntParameter(insertPreparedStatement, 11, travelTime);
        setIntParameter(insertPreparedStatement, 12, timeInLocation);

        // the copied fields
        setDoubleParameter(insertPreparedStatement, 13, key.mean_duration_factor.get(i));
        setDoubleParameter(insertPreparedStatement, 14, key.mean_duration_offset.get(i));
        setDoubleParameter(insertPreparedStatement, 15, key.safe_duration_factor.get(i));
        setDoubleParameter(insertPreparedStatement, 16, key.safe_duration_offset.get(i));
        batchTracker.addBatch();
    }
}
