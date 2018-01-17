package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.PatternFinder;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.loader.Field;
import com.conveyal.gtfs.loader.JdbcGtfsLoader;
import com.conveyal.gtfs.loader.Requirement;
import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.model.Pattern;
import com.conveyal.gtfs.model.PatternStop;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.ShapePoint;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import com.vividsolutions.jts.geom.Coordinate;
import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Groups trips together into "patterns" that share the same sequence of stops.
 * This is not a normal validator in the sense that it does not check for bad data.
 * It's taking advantage of the fact that we're already iterating over the trips one by one to build up the patterns.
 */
public class PatternFinderValidator extends TripValidator {

    private static final Logger LOG = LoggerFactory.getLogger(PatternFinderValidator.class);

    PatternFinder patternFinder;

    public PatternFinderValidator(Feed feed, SQLErrorStorage errorStorage) {
        super(feed, errorStorage);
        patternFinder = new PatternFinder();
    }

    @Override
    public void validateTrip (Trip trip, Route route, List<StopTime> stopTimes, List<Stop> stops, List<ShapePoint> shapePoints) {
        // As we hit each trip, accumulate them into the wrapped PatternFinder object.
        patternFinder.processTrip(trip, stopTimes, shapePoints);
    }

    /**
     * Store patterns and pattern stops in the database. Also, update the trips table with a pattern_id column.
     */
    @Override
    public void complete(ValidationResult validationResult) {
        LOG.info("Updating trips with pattern IDs...");
        // FIXME: There may be a better way to handle getting the full list of stops
        Map<String, Stop> stopById = new HashMap<>();
        for (Stop stop : feed.stops) {
            stopById.put(stop.stop_id, stop);
        }
        // FIXME In the editor we need patterns to exist separately from and before trips themselves, so me make another table.
        List<Pattern> patterns = patternFinder.createPatternObjects(stopById, errorStorage);
        Connection connection = null;
        try {
            // TODO this assumes gtfs-lib is using an SQL database and not a MapDB.
            // Maybe we should just create patterns in a separate step, but that would mean iterating over the stop_times twice.
            LOG.info("Storing pattern ID for each trip.");
            connection = feed.getConnection();
            Statement statement = connection.createStatement();
            String tripsTableName = feed.tablePrefix + "trips";
            String patternsTableName = feed.tablePrefix + "patterns";
            String patternStopsTableName = feed.tablePrefix + "pattern_stops";
//            String patternGeometryTableName = feed.tablePrefix + "pattern_geometry";
            statement.execute(String.format("alter table %s add column pattern_id varchar", tripsTableName));
            // FIXME: Here we're creating a pattern table that has an integer ID field (similar to the other GTFS tables)
            // and a varchar pattern_id with essentially the same value cast to a string. Perhaps the pattern ID should
            // be a UUID or something, just to better distinguish it from the int ID?
            statement.execute(String.format("create table %s (id serial, pattern_id varchar primary key, " +
                    "route_id varchar, name varchar, shape_id varchar)", patternsTableName));
            // FIXME: Use patterns table?
//            Table patternsTable = new Table(patternsTableName, Pattern.class, Requirement.EDITOR, Table.PATTERNS.fields);
            Table patternStopsTable = new Table(patternStopsTableName, PatternStop.class, Requirement.EDITOR,
                    Table.PATTERN_STOP.fields);
            String insertPatternStopSql = patternStopsTable.generateInsertSql(true);
            // Create pattern stops table with serial ID and primary key on pattern ID and stop sequence
            patternStopsTable.createSqlTable(connection, true, new String[]{"pattern_id", "stop_sequence"});
            PreparedStatement updateTripStatement = connection.prepareStatement(
                    String.format("update %s set pattern_id = ? where trip_id = ?", tripsTableName));
            PreparedStatement insertPatternStatement = connection.prepareStatement(
                    String.format("insert into %s values (DEFAULT, ?, ?, ?, ?)", patternsTableName));
            PreparedStatement insertPatternStopStatement = connection.prepareStatement(insertPatternStopSql);
            int batchSize = 0;
            // TODO update to use batch trackers
            for (Pattern pattern : patterns) {
                // First, create a pattern relation.
                insertPatternStatement.setString(1, pattern.pattern_id);
                insertPatternStatement.setString(2, pattern.route_id);
                insertPatternStatement.setString(3, pattern.name);
                insertPatternStatement.setString(4, pattern.associatedShapes.iterator().next());
                insertPatternStatement.addBatch();
                if (pattern.patternStops != null) {
                    // Next, add pattern_stops relations for all the stops on this pattern.
                    for (PatternStop patternStop : pattern.patternStops) {
                        for (int f = 0; f < patternStopsTable.fields.length; f++) {
                            Field field = patternStopsTable.fields[f];
                            String string;
                            try {
                                // FIXME Don't use reflection here.
                                java.lang.reflect.Field psField = patternStop.getClass().getField(field.getName());
                                Object value = psField.get(patternStop);
                                string = value != null ? value.toString() : null;
                                // No line number available because this is a derived object
                                patternStopsTable.setValueForField(insertPatternStopStatement, f - 1,
                                        -1, field, string, errorStorage);
                            } catch (IllegalAccessException | NoSuchFieldException e) {
                                LOG.error("Error accessing pattern stop field", e);
                            }
                        }
                        insertPatternStopStatement.addBatch();
                        batchSize += 1;
                    }
                }
                // Finally, update all trips on this pattern to reference this pattern's ID.
                for (String tripId : pattern.associatedTrips) {
                    updateTripStatement.setString(1, pattern.pattern_id);
                    updateTripStatement.setString(2, tripId);
                    updateTripStatement.addBatch();
                    batchSize += 1;
                }
                // If we've accumulated a lot of prepared statement calls, pass them on to the database backend.
                if (batchSize > JdbcGtfsLoader.INSERT_BATCH_SIZE) {
                    updateTripStatement.executeBatch();
                    insertPatternStatement.executeBatch();
                    insertPatternStopStatement.executeBatch();
                    batchSize = 0;
                }
            }
            // Send any remaining prepared statement calls to the database backend.
            updateTripStatement.executeBatch();
            insertPatternStatement.executeBatch();
            insertPatternStopStatement.executeBatch();
            // Index new pattern_id column on trips. The other tables are already indexed because they have primary keys.
            statement.execute(String.format("create index trips_pattern_id_idx on %s (pattern_id)", tripsTableName));
            connection.commit();
            connection.close();
            LOG.info("Done storing pattern IDs.");
        } catch (SQLException e) {
            // Close transaction if failure occurs on creating patterns.
            DbUtils.closeQuietly(connection);
            // This exception will be stored as a validator failure.
            throw new RuntimeException(e);
        }

    }

}

