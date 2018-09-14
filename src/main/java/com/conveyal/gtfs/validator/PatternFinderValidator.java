package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.PatternFinder;
import com.conveyal.gtfs.TripPatternKey;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.BatchTracker;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.loader.Requirement;
import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.model.Pattern;
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
    public void validateTrip (Trip trip, Route route, List<StopTime> stopTimes, List<Stop> stops) {
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
        for (Stop stop : feed.stops) {
            stopById.put(stop.stop_id, stop);
        }
        // FIXME In the editor we need patterns to exist separately from and before trips themselves, so me make another table.
        Map<TripPatternKey, Pattern> patterns = patternFinder.createPatternObjects(stopById, errorStorage);
        Connection connection = null;
        try {
            // TODO this assumes gtfs-lib is using an SQL database and not a MapDB.
            // Maybe we should just create patterns in a separate step, but that would mean iterating over the stop_times twice.
            LOG.info("Creating pattern and pattern stops tables.");
            connection = feed.getConnection();
            Statement statement = connection.createStatement();
            String tripsTableName = feed.tablePrefix + "trips";
            String patternsTableName = feed.tablePrefix + "patterns";
            String patternStopsTableName = feed.tablePrefix + "pattern_stops";
            statement.execute(String.format("alter table %s add column pattern_id varchar", tripsTableName));
            // FIXME: Here we're creating a pattern table that has an integer ID field (similar to the other GTFS tables)
            // AND a varchar pattern_id with essentially the same value cast to a string. Perhaps the pattern ID should
            // be a UUID or something, just to better distinguish it from the int ID?
            statement.execute(String.format("create table %s (id serial, pattern_id varchar, " +
                    "route_id varchar, name varchar, shape_id varchar)", patternsTableName));
            // FIXME: Use patterns table?
//            Table patternsTable = new Table(patternsTableName, Pattern.class, Requirement.EDITOR, Table.PATTERNS.fields);
            Table patternStopsTable = new Table(patternStopsTableName, PatternStop.class, Requirement.EDITOR,
                    Table.PATTERN_STOP.fields);
            String insertPatternStopSql = patternStopsTable.generateInsertSql(true);
            // Create pattern stops table with serial ID
            patternStopsTable.createSqlTable(connection, null, true);
            PreparedStatement insertPatternStatement = connection.prepareStatement(
                    String.format("insert into %s values (DEFAULT, ?, ?, ?, ?)", patternsTableName)
            );
            BatchTracker patternTracker = new BatchTracker("pattern", insertPatternStatement);
            PreparedStatement insertPatternStopStatement = connection.prepareStatement(insertPatternStopSql);
            BatchTracker patternStopTracker = new BatchTracker("pattern stop", insertPatternStopStatement);
            int currentPatternIndex = 0;
            LOG.info("Storing patterns and pattern stops");
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
            // TODO update to use batch trackers
            for (Map.Entry<TripPatternKey, Pattern> entry : patterns.entrySet()) {
                Pattern pattern = entry.getValue();
//                LOG.info("Batching pattern {}", pattern.pattern_id);
                TripPatternKey key = entry.getKey();
                // First, create a pattern relation.
                insertPatternStatement.setString(1, pattern.pattern_id);
                insertPatternStatement.setString(2, pattern.route_id);
                insertPatternStatement.setString(3, pattern.name);
                // FIXME: This could be null...
                String shapeId = pattern.associatedShapes.iterator().next();
                insertPatternStatement.setString(4, shapeId);
                patternTracker.addBatch();
                // Construct pattern stops based on values in trip pattern key.
                // FIXME: Use pattern stops table here?
                int lastValidDeparture = key.departureTimes.get(0);
                for (int i = 0; i < key.stops.size(); i++) {
                    int travelTime = 0;
                    String stopId = key.stops.get(i);
                    int arrival = key.arrivalTimes.get(i);
                    if (i > 0) {
                        int prevDeparture = key.departureTimes.get(i - 1);
                        // Set travel time for all stops except the first.
                        if (prevDeparture != INT_MISSING) {
                            // Update the previous departure if it's not missing. Otherwise, base travel time based on the
                            // most recent valid departure.
                            lastValidDeparture = prevDeparture;
                        }
                        travelTime = arrival == INT_MISSING || lastValidDeparture == INT_MISSING
                            ? INT_MISSING
                            : arrival - lastValidDeparture;
                    }
                    int departure = key.departureTimes.get(i);
                    int dwellTime = arrival == INT_MISSING || departure == INT_MISSING
                        ? INT_MISSING
                        : departure - arrival;

                    insertPatternStopStatement.setString(1, pattern.pattern_id);
                    setIntParameter(insertPatternStopStatement, 2, i);
                    insertPatternStopStatement.setString(3, stopId);
                    setIntParameter(insertPatternStopStatement,4, travelTime);
                    setIntParameter(insertPatternStopStatement,5, dwellTime);
                    setIntParameter(insertPatternStopStatement,6, key.dropoffTypes.get(i));
                    setIntParameter(insertPatternStopStatement,7, key.pickupTypes.get(i));
                    setDoubleParameter(insertPatternStopStatement, 8, key.shapeDistances.get(i));
                    setIntParameter(insertPatternStopStatement,9, key.timepoints.get(i));
                    patternStopTracker.addBatch();
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
            LOG.info("Done storing patterns and pattern stops.");
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
            DbUtils.closeQuietly(connection);
        }

    }

}

