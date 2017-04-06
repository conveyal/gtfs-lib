package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.model.*;
import com.conveyal.gtfs.storage.StorageException;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Iterator;

/**
 * This connects to an SQL RDBMS containing GTFS data and lets you fetch things out of it.
 *
 * Created by abyrd on 2017-04-04
 */
public class SQLFeed {

    private static final Logger LOG = LoggerFactory.getLogger(SQLFeed.class);

    TableReader<Route> routes;
    TableReader<Stop> stops;
    TableReader<Trip> trips;
    TableReader<ShapePoint> shapePoints;
    TableReader<StopTime> stopTimes;

    public SQLFeed (Connection connection) {
        routes      = new TableReader("routes",     connection, EntityPopulator.ROUTE);
        stops       = new TableReader("stops",      connection, EntityPopulator.STOP);
        trips       = new TableReader("trips",      connection, EntityPopulator.TRIP);
        shapePoints = new TableReader("shapes",     connection, EntityPopulator.SHAPE_POINT);
        stopTimes   = new TableReader("stop_times", connection, EntityPopulator.STOP_TIME);
    }

    public static void main (String[] params) {
        ConnectionSource connectionSource = new ConnectionSource(ConnectionSource.POSTGRES_LOCAL_URL);
        Connection connection = connectionSource.getConnection("nl");
        SQLFeed feed = new SQLFeed(connection);
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
