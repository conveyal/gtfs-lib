package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.error.GTFSError;
import com.conveyal.gtfs.model.*;
import com.conveyal.gtfs.validator.*;
import com.vividsolutions.jts.index.strtree.STRtree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * This connects to an SQL RDBMS containing GTFS data and lets you fetch things out of it.
 *
 * Created by abyrd on 2017-04-04
 */
public class Feed {

    private static final Logger LOG = LoggerFactory.getLogger(Feed.class);

    public final TableReader<Route> routes;
    public final TableReader<Stop>  stops;
    public final TableReader<Trip>  trips;
    public final TableReader<ShapePoint> shapePoints;
    public final TableReader<StopTime>   stopTimes;

    /* A place to accumulate errors while the feed is loaded. Tolerate as many errors as possible and keep on loading. */
    public final List<GTFSError> errors = new ArrayList<>();

    /**
     * Create a feed that reads tables over a JDBC connection. The connection should already be set to the right
     * schema within the database.
     */
    public Feed (Connection connection) {
        routes      = new JDBCTableReader(Table.ROUTES,     connection, EntityPopulator.ROUTE);
        stops       = new JDBCTableReader(Table.STOPS,      connection, EntityPopulator.STOP);
        trips       = new JDBCTableReader(Table.TRIPS,      connection, EntityPopulator.TRIP);
        shapePoints = new JDBCTableReader(Table.SHAPES,     connection, EntityPopulator.SHAPE_POINT);
        stopTimes   = new JDBCTableReader(Table.STOP_TIMES, connection, EntityPopulator.STOP_TIME);
    }

    /**
     * Create a feed that reads from a local MapDB.
     */
    public Feed (String feedVersionId, File cacheDir) {
        routes      = null;
        stops       = null;
        trips       = null;
        shapePoints = null;
        stopTimes   = null;
    }

    private STRtree stopSpatialIndex = null;

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
