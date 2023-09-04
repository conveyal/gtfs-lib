package com.conveyal.gtfs;

import com.conveyal.gtfs.loader.BatchTracker;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.loader.Requirement;
import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.model.Pattern;
import com.conveyal.gtfs.model.PatternStop;
import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Set;

import static com.conveyal.gtfs.loader.JdbcGtfsLoader.copyFromFile;
import static com.conveyal.gtfs.model.Entity.INT_MISSING;
import static com.conveyal.gtfs.model.Entity.setDoubleParameter;
import static com.conveyal.gtfs.model.Entity.setIntParameter;

public class PatternBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(PatternBuilder.class);

    private final Feed feed;
    private static final String TEMP_FILE_NAME = "pattern_for_trips";

    private final Connection connection;
    public PatternBuilder(Feed feed) throws SQLException {
        this.feed = feed;
        connection = feed.getConnection();
    }

    public void create(Map<TripPatternKey, Pattern> patterns, Set<String> patternIdsLoadedFromFile) {
        String patternsTableName = feed.getTableName("patterns");
        String tripsTableName = feed.getTableName("trips");
        String patternStopsTableName = feed.getTableName("pattern_stops");

        Table patternsTable = new Table(patternsTableName, Pattern.class, Requirement.EDITOR, Table.PATTERNS.fields);
        Table patternStopsTable = new Table(patternStopsTableName, PatternStop.class, Requirement.EDITOR, Table.PATTERN_STOP.fields);

        try {
            File tempPatternForTripsTextFile = File.createTempFile(TEMP_FILE_NAME, "text");
            LOG.info("Creating pattern and pattern stops tables.");
            Statement statement = connection.createStatement();
            statement.execute(String.format("alter table %s add column pattern_id varchar", tripsTableName));
            if (patternIdsLoadedFromFile.isEmpty()) {
                // No patterns were loaded from file so the pattern table has not previously been created.
                patternsTable.createSqlTable(connection, null, true);
            }
            patternStopsTable.createSqlTable(connection, null, true);
            try (PrintStream patternForTripsFileStream = createTempPatternForTripsTable(tempPatternForTripsTextFile, statement)) {
                processPatternAndPatternStops(patternsTable, patternStopsTable, patternForTripsFileStream, patterns, patternIdsLoadedFromFile);
            }
            updateTripPatternIds(tempPatternForTripsTextFile, statement, tripsTableName);
            createIndexes(statement, patternsTableName, patternStopsTableName, tripsTableName);
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

    private void processPatternAndPatternStops(
        Table patternsTable,
        Table patternStopsTable,
        PrintStream patternForTripsFileStream,
        Map<TripPatternKey, Pattern> patterns,
        Set<String> patternIdsLoadedFromFile
    ) throws SQLException {
        // Generate prepared statements for inserts.
        String insertPatternSql = patternsTable.generateInsertSql(true);
        PreparedStatement insertPatternStatement = connection.prepareStatement(insertPatternSql);
        BatchTracker patternTracker = new BatchTracker("pattern", insertPatternStatement);
        LOG.info("Storing patterns and pattern stops.");
        for (Map.Entry<TripPatternKey, Pattern> entry : patterns.entrySet()) {
            Pattern pattern = entry.getValue();
            LOG.debug("Batching pattern {}", pattern.pattern_id);
            if (!patternIdsLoadedFromFile.contains(pattern.pattern_id)) {
                // Only insert the pattern if it has not already been imported from file.
                pattern.setStatementParameters(insertPatternStatement, true);
                patternTracker.addBatch();
            }
            createPatternStops(entry.getKey(), pattern.pattern_id, patternStopsTable);
            updateTripPatternReferences(patternForTripsFileStream, pattern);
        }
        // Send any remaining prepared statement calls to the database backend.
        patternTracker.executeRemaining();
        LOG.info("Done storing patterns and pattern stops.");
    }

    /**
     * Create temp table for updating trips with pattern IDs to be dropped at the end of the transaction.
     * NOTE: temp table name must NOT be prefixed with schema because temp tables are prefixed with their own
     * connection-unique schema.
     */
    private PrintStream createTempPatternForTripsTable(
        File tempPatternForTripsTextFile,
        Statement statement
    ) throws SQLException, IOException {
        LOG.info("Loading via temporary text file at {}.", tempPatternForTripsTextFile.getAbsolutePath());
        String createTempSql = String.format("create temp table %s(trip_id varchar, pattern_id varchar) on commit drop", TEMP_FILE_NAME);
        LOG.info(createTempSql);
        statement.execute(createTempSql);
        return new PrintStream(new BufferedOutputStream(Files.newOutputStream(tempPatternForTripsTextFile.toPath())));
    }

    /**
     * Update all trips on this pattern to reference this pattern's ID.
     */
    private void updateTripPatternReferences(PrintStream patternForTripsFileStream, Pattern pattern) {
        // Prepare each trip in pattern to update trips table.
        for (String tripId : pattern.associatedTrips) {
            // Add line to temp csv file if using postgres.
            // No need to worry about null trip IDs because the trips have already been processed.
            String[] strings = new String[]{tripId, pattern.pattern_id};
            // Print a new line in the standard postgres text format:
            // https://www.postgresql.org/docs/9.1/static/sql-copy.html#AEN64380
            patternForTripsFileStream.println(String.join("\t", strings));
        }
    }

    /**
     * Copy the pattern for trips text file into a table, create an index on trip IDs, and update the trips
     * table.
     */
    private void updateTripPatternIds(
        File tempPatternForTripsTextFile,
        Statement statement,
        String tripsTableName
    ) throws SQLException, IOException {

        LOG.info("Updating trips with pattern IDs.");
        // Copy file contents into temp pattern for trips table.
        copyFromFile(connection, tempPatternForTripsTextFile, TEMP_FILE_NAME);
        // Before updating the trips with pattern IDs, index the table on trip_id.
        String patternForTripsIndexSql = String.format(
            "create index temp_trips_pattern_id_idx on %s (trip_id)",
            TEMP_FILE_NAME
        );
        LOG.info(patternForTripsIndexSql);
        statement.execute(patternForTripsIndexSql);
        // Finally, execute the update statement.
        String updateTripsSql = String.format(
            "update %s set pattern_id = %s.pattern_id from %s where %s.trip_id = %s.trip_id",
            tripsTableName,
            TEMP_FILE_NAME,
            TEMP_FILE_NAME,
            tripsTableName,
            TEMP_FILE_NAME
        );
        LOG.info(updateTripsSql);
        statement.executeUpdate(updateTripsSql);
        // Delete temp file. Temp table will be dropped after the transaction is committed.
        Files.delete(tempPatternForTripsTextFile.toPath());
        LOG.info("Updating trips complete.");
    }

    private void createIndexes(
        Statement statement,
        String patternsTableName,
        String patternStopsTableName,
        String tripsTableName
    ) throws SQLException {
        LOG.info("Creating index on patterns.");
        statement.executeUpdate(String.format("alter table %s add primary key (pattern_id)", patternsTableName));
        LOG.info("Creating index on pattern stops.");
        statement.executeUpdate(String.format("alter table %s add primary key (pattern_id, stop_sequence)", patternStopsTableName));
        // Index new pattern_id column on trips. The other tables are already indexed because they have primary keys.
        LOG.info("Indexing trips on pattern id.");
        statement.execute(String.format("create index trips_pattern_id_idx on %s (pattern_id)", tripsTableName));
        LOG.info("Done indexing.");
    }

    /**
     * Construct pattern stops based on values in trip pattern key.
     */
    private void createPatternStops(TripPatternKey key, String patternId, Table patternStopsTable) throws SQLException {

        String insertPatternStopSql = patternStopsTable.generateInsertSql(true);
        PreparedStatement insertPatternStopStatement = connection.prepareStatement(insertPatternStopSql);
        BatchTracker patternStopTracker = new BatchTracker("pattern stop", insertPatternStopStatement);

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

            insertPatternStopStatement.setString(1, patternId);
            // Stop sequence is zero-based.
            setIntParameter(insertPatternStopStatement, 2, i);
            insertPatternStopStatement.setString(3, stopId);
            insertPatternStopStatement.setString(4, key.stopHeadsigns.get(i));
            setIntParameter(insertPatternStopStatement,5, travelTime);
            setIntParameter(insertPatternStopStatement,6, dwellTime);
            setIntParameter(insertPatternStopStatement,7, key.dropoffTypes.get(i));
            setIntParameter(insertPatternStopStatement,8, key.pickupTypes.get(i));
            setDoubleParameter(insertPatternStopStatement, 9, key.shapeDistances.get(i));
            setIntParameter(insertPatternStopStatement,10, key.timepoints.get(i));
            setIntParameter(insertPatternStopStatement,11, key.continuous_pickup.get(i));
            setIntParameter(insertPatternStopStatement,12, key.continuous_drop_off.get(i));
            patternStopTracker.addBatch();
        }
        patternStopTracker.executeRemaining();
    }
}
