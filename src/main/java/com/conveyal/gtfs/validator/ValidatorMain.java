package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import com.conveyal.gtfs.storage.SqlLibrary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

/**
 * Example main class for examining a feed that has already been loaded into a JDBC SQL database.
 */
public class ValidatorMain {

    private static final Logger LOG = LoggerFactory.getLogger(ValidatorMain.class);

    /**
     * Example main method that validates an already-loaded feed, given its unique prefix (SQL schema name).
     */
    public static void main (String[] params) {
        if (params.length != 2) {
            LOG.info("Usage: ValidatorMain <unique_feed_prefix> <database_URL>");
        }
        String tablePrefix = params[0];
        String databaseUrl = params[1];

        // Ensure separator dot is present
        if (!tablePrefix.endsWith(".")) tablePrefix += ".";

        DataSource dataSource = SqlLibrary.createDataSource(databaseUrl);
        Feed feed = new Feed(dataSource, tablePrefix);
        feed.validate();


        if (params[0].equalsIgnoreCase("test")) {
            // TODO make this into a unit test
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
        }
    }


}
