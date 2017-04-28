package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.error.GTFSError;
import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.model.*;
import com.conveyal.gtfs.storage.StorageException;
import com.conveyal.gtfs.validator.*;
import com.google.common.collect.Iterables;
import com.vividsolutions.jts.index.strtree.STRtree;
import org.h2.command.Prepared;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This connects to an SQL RDBMS containing GTFS data and lets you fetch things out of it.
 *
 * Created by abyrd on 2017-04-04
 */
public class Feed {

    private static final Logger LOG = LoggerFactory.getLogger(Feed.class);

    private final Connection connection;

    public final TableReader<Route> routes;
    public final TableReader<Stop>  stops;
    public final TableReader<Trip>  trips;
    public final TableReader<ShapePoint> shapePoints;
    public final TableReader<StopTime>   stopTimes;

    /* A place to accumulate errors while the feed is loaded. Tolerate as many errors as possible and keep on loading. */
    // TODO remove this and use only NewGTFSErrors in Validators, loaded into a JDBC table
    public final List<GTFSError> errors = new ArrayList<>();

    /**
     * Create a feed that reads tables over a JDBC connection. The connection should already be set to the right
     * schema within the database.
     */
    public Feed (Connection connection) {
        this.connection = connection; // Should probably be a connection source.
        routes      = new JDBCTableReader(Table.ROUTES,     connection, EntityPopulator.ROUTE);
        stops       = new JDBCTableReader(Table.STOPS,      connection, EntityPopulator.STOP);
        trips       = new JDBCTableReader(Table.TRIPS,      connection, EntityPopulator.TRIP);
        shapePoints = new JDBCTableReader(Table.SHAPES,     connection, EntityPopulator.SHAPE_POINT);
        stopTimes   = new JDBCTableReader(Table.STOP_TIMES, connection, EntityPopulator.STOP_TIME);
    }

    /**
     * This will return a Feed object for the given GTFS feed file. It will load the data from the file into the Feed
     * object as needed, but will first look for a cached database file in the same directory and with the same name as
     * the GTFS feed file. This speeds up uses of the feed after the first time.
     */
    public Feed loadOrUseCached (String gtfsFilePath) {
        return null;
    }

    public static void main (String[] params) {
        ConnectionSource connectionSource = new ConnectionSource(ConnectionSource.POSTGRES_LOCAL_URL);
        Connection connection = connectionSource.getConnection(null);
        Feed feed = new Feed(connection);

        feed.validate();

        // TODO make this into a unit test
        if (params.length > 0 && params[0].equalsIgnoreCase("test")) {
            LOG.info("Start.");
            double x = 0;
            for (Route route : feed.routes) {
                x += route.route_type;
            }
            LOG.info("Done. {}", x);
            for (Stop stop : feed.stops) {
                x += stop.stop_lat;
            }
            LOG.info("Done. {}", x);
            for (Trip trip : feed.trips) {
                x += trip.direction_id;
            }
            LOG.info("Done. {}", x);
    //        for (ShapePoint shapePoint : feed.shapePoints) {
    //            x += shapePoint.shape_dist_traveled;
    //        }
    //        LOG.info("Done. {}", x);
            // It takes about 25 seconds to iterate over all stop times,
            // as opposed to 83 seconds to iterate over all stop times in order for each trip.
            for (StopTime stopTime : feed.stopTimes) {
                x += stopTime.shape_dist_traveled;
            }
            LOG.info("Done. {}", x);
            return;
        }


    }

    private void validate (boolean repair, FeedValidator... feedValidators) {
        createErrorTable();
        long validationStartTime = System.currentTimeMillis();
        for (FeedValidator feedValidator : feedValidators) {
            try {
                LOG.info("Running {}.", feedValidator.getClass().getSimpleName());
                long startTime = System.currentTimeMillis();
                feedValidator.validate(this, repair);
                long endTime = System.currentTimeMillis();
                long diff = endTime - startTime;
                LOG.info("{} finished in {} milliseconds.", feedValidator.getClass().getSimpleName(), diff);
                LOG.info("{} found {} errors.", feedValidator.getClass().getSimpleName(), feedValidator.getErrorCount());
                LOG.info("Storing {} errors...", feedValidator.getErrorCount());
                saveErrors(feedValidator.getErrors());
            } catch (Exception e) {
                LOG.error("{} failed.", feedValidator.getClass().getSimpleName());
                LOG.error(e.toString());
                e.printStackTrace();
            }
        }
        long validationEndTime = System.currentTimeMillis();
        long totalValidationTime = validationEndTime - validationStartTime;
        LOG.info("{} validators completed in {} milliseconds.", feedValidators.length, totalValidationTime);
    }

    private void createErrorTable () { // TODO move table creation to initial load?
        try {
            Statement statement = connection.createStatement();
            // Order in which tables are dropped matters because of foreign keys
            statement.execute("drop table if exists error_info");
            statement.execute("drop table if exists error_refs");
            statement.execute("drop table if exists errors");
            statement.execute("create table errors (error_id integer primary key, type varchar, problems varchar)");
            statement.execute("create table error_refs (error_id integer, entity_type varchar, line_number integer, entity_id varchar, sequence_number integer)");
            statement.execute("create table error_info (error_id integer, key varchar, value varchar)");
            connection.commit();
        } catch (SQLException ex) {
            throw new StorageException(ex);
        }
    }

    private static final long INSERT_BATCH_SIZE = 500;
    private int errorId = 0; // FIXME do we really want to use an instance field for this? It needs to across calls.

    // TODO errorStorage abstraction - pass into validators to stream errors into database without any intermediate storage. OR: just use intermediate storage and insert them all at the end.

    public void saveErrors (Iterable<NewGTFSError> errors) throws SQLException {
        PreparedStatement insertError = connection.prepareStatement("insert into errors values (?, ?, ?)");
        PreparedStatement insertRef =   connection.prepareStatement("insert into error_refs values (?, ?, ?, ?, ?)");
        PreparedStatement insertInfo =  connection.prepareStatement("insert into error_info values (?, ?, ?)");
        for (NewGTFSError error : errors) {
            // Insert one row for the error itself
            insertError.setInt(1, errorId);
            insertError.setString(2, error.type.name());
            insertError.setString(3, error.badValues);
            insertError.addBatch();
            // Insert rows for informational key-value pairs for this error
            // [NOT IMPLEMENTED, USING STRINGS]
            // Insert rows for entities referenced by this error
            for (NewGTFSError.EntityReference ref : error.referencedEntities) {
                insertRef.setInt(1, errorId);
                insertRef.setString(2, ref.type.getSimpleName());
                // TODO handle missing (-1?) We generate these so we can safely use negative to mean missing.
                insertRef.setInt(3, ref.lineNumber);
                insertRef.setString(4, ref.id);
                // TODO are seq numbers constrained to be positive? If so we don't need to use objects.
                if (ref.sequenceNumber == null) insertRef.setNull(5, Types.INTEGER);
                else insertRef.setInt(5, ref.sequenceNumber);
                insertRef.addBatch();
            }
            if (errorId % INSERT_BATCH_SIZE == 0) {
                insertError.executeBatch();
                insertRef.executeBatch();
                insertInfo.executeBatch();
            }
            errorId += 1;
        }
        // Execute any remaining batch inserts and commit the transaction.
        insertError.executeBatch();
        insertRef.executeBatch();
        insertInfo.executeBatch();
        connection.commit();
    }

    public void validate () {
        validate(false,
                new MisplacedStopValidator(),
                new DuplicateStopsValidator(),
                new TimeZoneValidator(),
                new NewTripTimesValidator(),
                new NamesValidator()
        );
    }

}
