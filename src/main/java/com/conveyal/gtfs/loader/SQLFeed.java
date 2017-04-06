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

    Connection connection;

    // Bundle all this into a genericized entity selector/iterator class

    PreparedStatement selectAllRoutes;

    PreparedStatement selectAllStops;

    PreparedStatement selectAllTrips;

    PreparedStatement selectAllShapePoints;

    PreparedStatement selectAllStopTimes;

    /**
     *
     * @param url the JDBC connection URL. (could add schema here too but it's not standard)
     * @param feedVersionId the unique identifier of the GTFS feed version that you want to access.
     *                      This will be prefixed on the GTFS table names. If null, uses the public schema.
     */
    public SQLFeed (String url, String feedVersionId) {
        try {
            // JODBC drivers should auto-register these days. You used to have to trick the class loader into loading them.
            connection = DriverManager.getConnection(url);
            if (feedVersionId == null) feedVersionId = "public";
            // We could also just prefix the table names with the feedVersionId and a dot, but this is a bit cleaner.
            connection.setSchema(feedVersionId);
            // Setting fetchSize to something other than zero enables server-side cursor use.
            // This will only be effective if autoCommit=false though. Otherwise it fills up the memory with all rows.
            // By default prepared statements are forward-only and read-only, but it wouldn't hurt to show that explicitly.
            connection.setAutoCommit(false);
            selectAllRoutes = connection.prepareStatement("select * from routes");
            selectAllStops  = connection.prepareStatement("select * from stops");
            selectAllTrips  = connection.prepareStatement("select * from trips");
            selectAllShapePoints = connection.prepareStatement("select * from shapes");
            selectAllShapePoints.setFetchSize(5000);
            selectAllStopTimes = connection.prepareStatement("select * from stop_times");
            selectAllStopTimes.setFetchSize(5000);
        } catch (Exception ex) {
            throw new StorageException(ex);
        }
    }

    public EntityIterator<Route> routeIterator () {
        return new EntityIterator<>(selectAllRoutes, EntityPopulator.ROUTE);
    }

    public EntityIterator<Stop> stopIterator () {
        return new EntityIterator<>(selectAllStops, EntityPopulator.STOP);
    }

    public EntityIterator<Trip> tripIterator () {
        return new EntityIterator<>(selectAllTrips, EntityPopulator.TRIP);
    }

    public EntityIterator<ShapePoint> shapePointIterator () {
        return new EntityIterator<>(selectAllShapePoints, EntityPopulator.SHAPE_POINT);
    }

    public EntityIterator<StopTime> stopTimeIterator () {
        return new EntityIterator<>(selectAllStopTimes, EntityPopulator.STOP_TIME);
    }

    // FIXME it seems kind of wrong to implement iterable and iterator on the same object.
    // Really we should have an iterable SQLEntityCollection or fetcher that also supports single-fetch.
    private class EntityIterator <T extends Entity> implements Iterator<T>, Iterable<T> {

        boolean hasMoreEntities;
        ResultSet results;
        TObjectIntMap<String> columnForName;
        EntityPopulator<T> entityPopulator;

        EntityIterator (PreparedStatement preparedStatement, EntityPopulator<T> entityPopulator) {
            this.entityPopulator = entityPopulator;
            try {
                results = preparedStatement.executeQuery();
                hasMoreEntities = results.next();
                ResultSetMetaData metaData = results.getMetaData();
                int nColumns = metaData.getColumnCount();
                // No entry value defaults to zero, and SQL columns are 1-based.
                // Cache the index for each column name to avoid throwing exceptions for missing ones.
                columnForName = new TObjectIntHashMap<>(nColumns);
                for (int c = 1; c <= nColumns; c++) columnForName.put(metaData.getColumnName(c), c);
            } catch (Exception ex) {
                throw new StorageException(ex);
            }
        }

        @Override
        public boolean hasNext () {
            return hasMoreEntities;
        }

        @Override
        public T next() {
            try {
                T entity = entityPopulator.populate(results, columnForName);
                hasMoreEntities = results.next();
                if (!hasMoreEntities) {
                    results.close();
                    connection.commit();
                }
                return entity;
            } catch (Exception ex) {
                throw new StorageException(ex);
            }
        }

        @Override
        public Iterator<T> iterator() {
            return this;
        }

    }

    public static void main (String[] params) {
        SQLFeed feed = new SQLFeed(CsvLoader.POSTGRES_LOCAL_URL, null);
        LOG.info("Start.");
        double x = 0;
        for (Route route : feed.routeIterator()) {
            x += route.route_type;
        }
        LOG.info("Done. {}", x);
        for (Stop stop : feed.stopIterator()) {
            x += stop.stop_lat;
        }
        LOG.info("Done. {}", x);
        for (Trip trip : feed.tripIterator()) {
            x += trip.direction_id;
        }
        LOG.info("Done. {}", x);
        for (ShapePoint shapePoint : feed.shapePointIterator()) {
            x += shapePoint.shape_dist_traveled;
        }
        LOG.info("Done. {}", x);
        for (StopTime stopTime : feed.stopTimeIterator()) {
            x += stopTime.shape_dist_traveled;
        }
        LOG.info("Done. {}", x);
    }

}
