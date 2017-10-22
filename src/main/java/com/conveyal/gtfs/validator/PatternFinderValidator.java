package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.PatternFinder;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.Pattern;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

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
    public void validateTrip (Trip trip, Route route, List<StopTime> stopTimes, List<Stop> stops) {
        // As we hit each trip, accumulate them into the wrapped PatternFinder object.
        patternFinder.processTrip(trip, stopTimes);
    }

    @Override
    public void complete(ValidationResult validationResult) {
        LOG.info("Updating trips with pattern IDs...");
        // Return patterns in the result
        // TODO should we really return them in the validation result, or just put them in the DB?
        // The thing is, in a relational database this would require at least two tables and extra columns in the trips.
        // patterns, stopsInPatterns, patternForTrip. Though we could in fact get the stop sequence from any example trip in the pattern.
        // FIXME In the editor we need patterns to exist separately from and before trips themselves, so me make another table.
        validationResult.patterns = patternFinder.createPatternObjects();
        try {
            // TODO this assumes gtfs-lib is using an SQL database and not a MapDB.
            // Maybe we should just create patterns in a separate step, but that would mean iterating over the stop_times twice.
            LOG.info("Storing pattern ID for each trip.");
            Connection connection = feed.getConnection();
            Statement statement = connection.createStatement();
            String tripsTableName = feed.tablePrefix + "trips";
            String patternsTableName = feed.tablePrefix + "patterns";
            String patternStopsTableName = feed.tablePrefix + "pattern_stops";
            statement.execute(String.format("alter table %s add column pattern_id varchar", tripsTableName));
            statement.execute(String.format(
                    "create table %s (pattern_id varchar primary key, route_id varchar, description varchar)",
                    patternsTableName));
            statement.execute(String.format(
                    "create table %s (pattern_id varchar, stop_sequence integer, stop_id varchar, " +
                            "primary key (pattern_id, stop_sequence))",
                    patternStopsTableName));
            PreparedStatement updateTripStatement = connection.prepareStatement(
                    String.format("update %s set pattern_id = ? where trip_id = ?", tripsTableName));
            PreparedStatement insertPatternStatement = connection.prepareStatement(
                    String.format("insert into %s values (?, ?)", patternsTableName));
            PreparedStatement insertPatternStopStatement = connection.prepareStatement(
                    String.format("insert into %s values (?, ?, ?)", patternStopsTableName));
            int batchSize = 0;
            for (Pattern pattern : validationResult.patterns) {
                // First, create a pattern relation.
                insertPatternStatement.setString(1, pattern.pattern_id);
                insertPatternStatement.setString(2, pattern.route_id);
                insertPatternStatement.addBatch();
                // Next, add pattern_stops relations for all the stops on this pattern.
                int stop_sequence = 0;
                for (String stop_id : pattern.orderedStops) {
                    insertPatternStopStatement.setString(1, pattern.pattern_id);
                    insertPatternStopStatement.setInt(2, stop_sequence);
                    insertPatternStopStatement.setString(3, stop_id);
                    insertPatternStopStatement.addBatch();
                    batchSize += 1;
                    stop_sequence += 1;
                }
                // Finally, update all trips on this pattern to reference this pattern's ID.
                for (String tripId : pattern.associatedTrips) {
                    updateTripStatement.setString(1, pattern.pattern_id);
                    updateTripStatement.setString(2, tripId);
                    updateTripStatement.addBatch();
                    batchSize += 1;
                }
                // If we've accumulated a lot of prepared statement calls, pass them on to the database backend.
                if (batchSize % 1000 == 0) {
                    updateTripStatement.executeBatch();
                    insertPatternStatement.executeBatch();
                    insertPatternStopStatement.executeBatch();
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
            throw new RuntimeException(e);
        }

    }

}

