package com.conveyal.gtfs.storage;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.model.*;
import com.sun.org.apache.xpath.internal.operations.Gt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * Created by abyrd on 2017-03-25
 */
public class Backend {

    private static final Logger LOG = LoggerFactory.getLogger(Backend.class);

    // private static final String URL = "jdbc:h2:file:~/test-db";
    // private static final String URL = "jdbc:h2:mem:";
    private static final String URL = "jdbc:postgresql://localhost/catalogue";

    private Connection connection = null;

    private Statement statement;

    private Map<Class, ObjectRelationalMapping<Entity>> mappings = new HashMap<>();

    // TESTING ONLY
    public static Backend instance = new Backend();

    public Backend () {
        try {
            // Driver should auto-register these days.
            connection = DriverManager.getConnection(URL);
            // connection.setSchema()
            // connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            statement = connection.createStatement();
            statement.execute("create schema if not exists nl");
            connection.setAutoCommit(false);
            statement = connection.createStatement();
            mappings.put(Stop.class, new ObjectRelationalMapping(Stop.class, "nl.stops", connection));
            mappings.put(Route.class, new ObjectRelationalMapping(Route.class, "nl.routes", connection));
            mappings.put(Trip.class, new ObjectRelationalMapping(Trip.class, "nl.trips", connection));
            mappings.put(StopTime.class, new ObjectRelationalMapping(StopTime.class, "nl.stop_times", connection));
            // Tables are currently being created when mappings are instantiated.
            //for (ObjectRelationalMapping mapping : mappings.values()) mapping.createTable();
            connection.commit();
        } catch (Exception ex) {
            throw new StorageException(ex);
        }
    }

    public static void main(String args[]) throws Exception {
//        Backend backend = new Backend();
//        backend.setup();
        GTFSFeed feed = GTFSFeed.fromFile("/Users/abyrd/geodata/nl/NL-OPENOV-20170322-gtfs.zip");
        LOG.info("COMMITTING");
        Backend.instance.commit();
        LOG.info("DONE");
        LOG.info("INDEXING");
        Backend.instance.connection.setAutoCommit(true);
        Statement statement = Backend.instance.connection.createStatement();
        statement.execute("create index on stop_times (trip_id, stop_sequence)");
        LOG.info("DONE INDEXING");
    }

    public void commit () {
        try {
            connection.commit();
        } catch (Exception ex) {
            throw new StorageException(ex);
        }
    }

    public void store (Entity entity) {
        Class objectClass = entity.getClass();
        ObjectRelationalMapping orm = mappings.get(objectClass);
//        if (orm == null) {
//            orm = new ObjectRelationalMapping(objectClass, objectClass.getSimpleName(), connection);
//            orm.createTable();
//            mappings.put(objectClass, orm);
//        }
        if (orm == null) {
            throw new StorageException("Attempted to store an object of an unregistered type.");
        }
        orm.store(entity);
    }

}
