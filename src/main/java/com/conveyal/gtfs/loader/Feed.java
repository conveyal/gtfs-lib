package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.error.GTFSError;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.model.*;
import com.conveyal.gtfs.validator.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;

/**
 * This connects to an SQL RDBMS containing GTFS data and lets you fetch elements out of it.
 */
public class Feed {

    private static final Logger LOG = LoggerFactory.getLogger(Feed.class);

    private final DataSource dataSource;

    private final String tablePrefix; // including any separator character, may be the empty string.

    public final TableReader<Agency> agencies;
//    public final TableReader<Fare> fares;
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
     * @param tablePrefix the unique prefix for the table (may be null for no prefix)
     */
    public Feed (DataSource dataSource, String tablePrefix) {
        this.dataSource = dataSource;
        // Ensure separator dot is present
        if (tablePrefix != null && !tablePrefix.endsWith(".")) tablePrefix += ".";
        this.tablePrefix = tablePrefix == null ? "" : tablePrefix;
        agencies = new JDBCTableReader(Table.AGENCY, dataSource, tablePrefix, EntityPopulator.AGENCY);
//        fares = new JDBCTableReader(Table.FARES, dataSource, tablePrefix, EntityPopulator.FARE);
        routes = new JDBCTableReader(Table.ROUTES, dataSource, tablePrefix, EntityPopulator.ROUTE);
        stops = new JDBCTableReader(Table.STOPS, dataSource, tablePrefix, EntityPopulator.STOP);
        trips = new JDBCTableReader(Table.TRIPS, dataSource, tablePrefix, EntityPopulator.TRIP);
        shapePoints = new JDBCTableReader(Table.SHAPES, dataSource, tablePrefix, EntityPopulator.SHAPE_POINT);
        stopTimes = new JDBCTableReader(Table.STOP_TIMES, dataSource, tablePrefix, EntityPopulator.STOP_TIME);
    }

    /**
     * This will return a Feed object for the given GTFS feed file. It will load the data from the file into the Feed
     * object as needed, but will first look for a cached database file in the same directory and with the same name as
     * the GTFS feed file. This speeds up uses of the feed after the first time.
     */
    public Feed loadOrUseCached (String gtfsFilePath) {
        return null;
    }

    private void validate (SQLErrorStorage errorStorage, FeedValidator... feedValidators) {
        long validationStartTime = System.currentTimeMillis();
        for (FeedValidator feedValidator : feedValidators) {
            try {
                LOG.info("Running {}.", feedValidator.getClass().getSimpleName());
                int errorCountBefore = errorStorage.getErrorCount();
                feedValidator.validate();
                LOG.info("{} found {} errors.", feedValidator.getClass().getSimpleName(), errorStorage.getErrorCount() - errorCountBefore);
            } catch (Exception e) {
                LOG.error("{} failed.", feedValidator.getClass().getSimpleName());
                LOG.error(e.toString());
                e.printStackTrace();
            }
        }
        LOG.info("Total number of errors found by all validators: {}", errorStorage.getErrorCount());
        errorStorage.commitAndClose();
        long validationEndTime = System.currentTimeMillis();
        long totalValidationTime = validationEndTime - validationStartTime;
        LOG.info("{} validators completed in {} milliseconds.", feedValidators.length, totalValidationTime);
    }

    /**
     * TODO check whether validation has already occurred, overwrite results.
     */
    public void validate () {
        // Error tables should already be present from the initial load.
        // Reconnect to the existing error tables.
        SQLErrorStorage errorStorage = new SQLErrorStorage(dataSource, tablePrefix, false);
        validate (errorStorage,
            new MisplacedStopValidator(this, errorStorage),
            new DuplicateStopsValidator(this, errorStorage),
            new TimeZoneValidator(this, errorStorage),
            new NewTripTimesValidator(this, errorStorage),
            new NamesValidator(this, errorStorage)
        );
    }

    public void close () {
        LOG.info("Closing feed connections for {}", tablePrefix);
        routes.close();
        stops.close();
        trips.close();
        shapePoints.close();
        stopTimes.close();
    }

}
