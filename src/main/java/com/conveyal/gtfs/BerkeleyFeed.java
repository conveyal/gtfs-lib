package com.conveyal.gtfs;

import com.conveyal.gtfs.error.GTFSError;
import com.conveyal.gtfs.model.Agency;
import com.conveyal.gtfs.model.Calendar;
import com.conveyal.gtfs.model.CalendarDate;
import com.conveyal.gtfs.model.Fare;
import com.conveyal.gtfs.model.FareAttribute;
import com.conveyal.gtfs.model.FareRule;
import com.conveyal.gtfs.model.FeedInfo;
import com.conveyal.gtfs.model.Frequency;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.Service;
import com.conveyal.gtfs.model.ShapePoint;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Transfer;
import com.conveyal.gtfs.model.Trip;
import com.conveyal.gtfs.model.TripAndSequence;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class BerkeleyFeed implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(BerkeleyFeed.class);

    private final Environment dbEnvironment;
    private final EntityStore entityStore;

    public final PrimaryIndex<String, Trip> trips;
    public final PrimaryIndex<String, Stop> stops;
    public final PrimaryIndex<TripAndSequence, StopTime> stopTimes;

    public Set<GTFSError> errors = new HashSet<>();

    public BerkeleyFeed () {
        // Open the environment. Create it if it does not already exist.
        // Maybe this should be shared across several databases.
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        dbEnvironment = new Environment(new File("/Users/abyrd/temp/berkeleydb"), envConfig);

        // Entity Store
        StoreConfig storeConfig = new StoreConfig();
        storeConfig.setAllowCreate(true);
        storeConfig.setDeferredWrite(true);
        entityStore = new EntityStore(dbEnvironment, "TriMet Feed", storeConfig);

        // Create one index per GTFS entity type
        trips = entityStore.getPrimaryIndex(String.class, Trip.class);
        stops = entityStore.getPrimaryIndex(String.class, Stop.class);
        stopTimes = entityStore.getPrimaryIndex(TripAndSequence.class, StopTime.class);
    }

    public void close () {
        LOG.info("Closing database...");
        if (entityStore != null) {
            try {
                entityStore.close();
            } catch(DatabaseException dbe) {
                System.err.println("Error closing entity store: " + dbe.toString());
                System.exit(-1);
            }
        }
        if (dbEnvironment != null) {
            try {
                // Finally, close environment.
                dbEnvironment.sync();
                dbEnvironment.close();
            } catch(DatabaseException dbe) {
                System.err.println("Error closing database environment: " + dbe.toString());
                System.exit(-1);
            }
        }
        LOG.info("Closed.");
    }

    /**
     * The order in which we load the tables is important because referenced entities must be loaded before any
     * entities that reference them. This is because we check referential integrity while the files are being loaded.
     * This is done on the fly during loading because it allows us to associate a line number with errors in objects
     * that don't have any other clear identifier.
     *
     * Interestingly, all references are resolvable when tables are loaded in alphabetical order.
     */
    public void loadFromFile(String gtfsFile) throws Exception {

        ZipFile zip = new ZipFile(gtfsFile);
        new Stop.BLoader(this).loadTable(zip);
        new Trip.BLoader(this).loadTable(zip);
        new StopTime.BLoader(this).loadTable(zip);

    }

    public static void main (String[] args) {
        try (BerkeleyFeed feed = new BerkeleyFeed()) {
            //feed.loadFromFile("/Users/abyrd/gtfs/trimet-2019-01-24.gtfs.zip");
            GTFSFeed f2 = new GTFSFeed("/Users/abyrd/temp/testmapdb");
            f2.loadFromFile(new ZipFile("/Users/abyrd/gtfs/trimet-2019-01-24.gtfs.zip"));

        } catch (Exception ex) {
            LOG.error("Exception while testing.", ex);
        }
    }

}
