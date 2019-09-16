package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.model.Calendar;
import com.conveyal.gtfs.model.CalendarDate;
import com.conveyal.gtfs.model.Entity;
import com.conveyal.gtfs.model.FareAttribute;
import com.conveyal.gtfs.model.FareRule;
import com.conveyal.gtfs.model.FeedInfo;
import com.conveyal.gtfs.model.Frequency;
import com.conveyal.gtfs.storage.StorageException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static com.conveyal.gtfs.error.NewGTFSErrorType.*;
import static com.conveyal.gtfs.util.Util.randomIdString;

/**
 * This class loads a MapDB-based GTFSFeed into an RDBMS.
 */
public class JdbcGTFSFeedConverter {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcGTFSFeedConverter.class);

    private final GTFSFeed gtfsFeed;
    private final DataSource dataSource;

    // These fields will be filled in once feed loading begins.
    private Connection connection;
    private String tablePrefix;
    private SQLErrorStorage errorStorage;

    private String namespace;

    // FIXME Add parameter for Connection so that this can be a part of a larger transaction.
    public JdbcGTFSFeedConverter(GTFSFeed gtfsFeed, DataSource dataSource) {
        this.gtfsFeed = gtfsFeed;
        this.dataSource = dataSource;
    }


    // Hash to uniquely identify files.
    // We can't use CRC32, the probability of collision on 10k items is about 1%.
    // https://stackoverflow.com/a/1867252
    // http://preshing.com/20110504/hash-collision-probabilities/
    // On the full NL feed:
    // MD5 took 820 msec,    cabb18e43798f92c52d5d0e49f52c988
    // Murmur took 317 msec, 5e5968f9bf5e1cdf711f6f48fcd94355
    // SHA1 took 1072 msec,  9fb356af4be2750f20955203787ec6f95d32ef22

    // There appears to be no advantage to loading tables in parallel, as the whole loading process is I/O bound.
    public FeedLoadResult loadTables () {

        // This result object will be returned to the caller to summarize the feed and report any critical errors.
        FeedLoadResult result = new FeedLoadResult();

        try {
            // Begin tracking time. FIXME: should this follow the connect/register and begin with the table loads?
            long startTime = System.currentTimeMillis();
            // We get a single connection object and share it across several different methods.
            // This ensures that actions taken in one method are visible to all subsequent SQL statements.
            // If we create a schema or table on one connection, then access it in a separate connection, we have no
            // guarantee that it exists when the accessing statement is executed.
            connection = dataSource.getConnection();
            // Generate a unique prefix that will identify this feed.
            // Prefixes ("schema" names) based on feed_id and feed_version get very messy, so we use random unique IDs.
            // We don't want to use an auto-increment numeric primary key because these need to be alphabetical.
            // Although ID collisions are theoretically possible, they are improbable in the extreme because our IDs
            // are long enough to have as much entropy as a UUID. So we don't really need to check for uniqueness and
            // retry in a loop.
            // TODO handle the case where we don't want any prefix.
            this.tablePrefix = this.namespace = randomIdString();
            result.uniqueIdentifier = tablePrefix;
            registerFeed();
            // Include the dot separator in the table prefix.
            // This allows everything to work even when there's no prefix.
            this.tablePrefix += ".";
            this.errorStorage = new SQLErrorStorage(connection, tablePrefix, true);

            // Get entity lists that are nested in feed.
            List<Calendar> calendars = gtfsFeed.services.values()
                    .stream()
                    .map(service -> service.calendar)
                    .collect(Collectors.toList());
            List<CalendarDate> calendarDates = gtfsFeed.services.values()
                    .stream()
                    .map(service -> service.calendar_dates.values())
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
            List<FareAttribute> fareAttributes = gtfsFeed.fares.values()
                    .stream()
                    .map(fare -> fare.fare_attribute)
                    .collect(Collectors.toList());
            List<FareRule> fareRules = gtfsFeed.fares.values()
                    .stream()
                    .map(fare -> fare.fare_rules)
                    .flatMap(List::stream)
                    .collect(Collectors.toList());
            List<Frequency> frequencies = gtfsFeed.frequencies.stream()
                    .map(stringFrequencyTuple2 -> stringFrequencyTuple2.b)
                    .collect(Collectors.toList());

            // Copy all tables (except for PATTERN_STOPS, which does not exist in GTFSFeed).
            copyEntityToSql(gtfsFeed.agency.values(), Table.AGENCY);
            copyEntityToSql(calendars, Table.CALENDAR);
            copyEntityToSql(calendarDates, Table.CALENDAR_DATES);
            copyEntityToSql(gtfsFeed.routes.values(), Table.ROUTES);
            copyEntityToSql(gtfsFeed.patterns.values(), Table.PATTERNS);
            // FIXME: How to handle pattern stops?
//            copyEntityToSql(gtfsFeed.patterns.values(), Table.PATTERN_STOP);
            copyEntityToSql(fareAttributes, Table.FARE_ATTRIBUTES);
            copyEntityToSql(fareRules, Table.FARE_RULES);
            copyEntityToSql(gtfsFeed.feedInfo.values(), Table.FEED_INFO);
            copyEntityToSql(gtfsFeed.shape_points.values(), Table.SHAPES);
            copyEntityToSql(gtfsFeed.stops.values(), Table.STOPS);
            copyEntityToSql(gtfsFeed.transfers.values(), Table.TRANSFERS);
            copyEntityToSql(gtfsFeed.trips.values(), Table.TRIPS); // refs routes
            copyEntityToSql(frequencies, Table.FREQUENCIES); // refs trips
            copyEntityToSql(gtfsFeed.stop_times.values(), Table.STOP_TIMES);
//            result.errorCount = errorStorage.getErrorCount();
            // This will commit and close the single connection that has been shared between all preceding load steps.
            errorStorage.commitAndClose();
            result.completionTime = System.currentTimeMillis();
            result.loadTimeMillis = result.completionTime - startTime;
            LOG.info("Loading tables took {} sec", result.loadTimeMillis / 1000);
        } catch (Exception ex) {
            try {
                connection.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            // TODO catch exceptions separately while loading each table so load can continue, store in TableLoadResult
            LOG.error("Exception while loading GTFS file: {}", ex.toString());
            ex.printStackTrace();
            result.fatalException = ex.getMessage();
        } finally {
            DbUtils.closeQuietly(connection);
        }
        return result;
    }

    /**
     * Add a line to the list of loaded feeds showing that this feed has been loaded.
     * We used to inspect feed_info here so we could make our table prefix based on feed ID and version.
     * Now we just load feed_info like any other table.
     *         // Create a row in the table of loaded feeds for this feed
     * Really this is not just making the table prefix - it's loading the feed_info and should also calculate hashes.
     *
     * Originally we were flattening all feed_info files into one root-level table, but that forces us to drop any
     * custom fields in feed_info.
     */
    private void registerFeed () {

        // FIXME is this extra CSV reader used anymore? Check comment below.
        // First, inspect feed_info.txt to extract the ID and version.
        // We could get this with SQL after loading, but feed_info, feed_id and feed_version are all optional.
        FeedInfo feedInfo = gtfsFeed.feedInfo.isEmpty() ? null : gtfsFeed.feedInfo.values().iterator().next();
        String feedId = "", feedVersion = "";
        if (feedInfo != null) {
            feedId = feedInfo.feed_id;
            feedVersion = feedInfo.feed_version;
        }

        try {
            Statement statement = connection.createStatement();
            // FIXME do the following only on databases that support schemas.
            // SQLite does not support them. Is there any advantage of schemas over flat tables?
            statement.execute("create schema " + tablePrefix);
            // current_timestamp seems to be the only standard way to get the current time across all common databases.
            // Record total load processing time?
            statement.execute("create table if not exists feeds (namespace varchar primary key, md5 varchar, " +
                    "sha1 varchar, feed_id varchar, feed_version varchar, filename varchar, loaded_date timestamp, " +
                    "snapshot_of varchar)");
            PreparedStatement insertStatement = connection.prepareStatement(
                    "insert into feeds values (?, ?, ?, ?, ?, ?, current_timestamp, null, false)");
            insertStatement.setString(1, tablePrefix);
            insertStatement.setString(2, null); // md5Hex
            insertStatement.setString(3, null); // shaHex
            // FIXME: will this throw an NPE if feedId or feedVersion are empty?
            insertStatement.setString(4, feedId.isEmpty() ? null : feedId);
            insertStatement.setString(5, feedVersion.isEmpty() ? null : feedVersion);
            insertStatement.setString(6, "mapdb_gtfs_feed"); // filename
            insertStatement.execute();
            connection.commit();
            LOG.info("Created new feed namespace: {}", insertStatement);
        } catch (Exception ex) {
            try {
                connection.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            LOG.error("Exception while registering new feed namespace in feeds table: {}", ex.getMessage());
            DbUtils.closeQuietly(connection);
        }
    }

    /**
     * Creates table for the specified Table, inserts all entities for the iterable in batches, and, finally, creates
     * indexes on the table.
     */
    private <E extends Entity> void copyEntityToSql(Iterable<E> entities, Table table) throws SQLException {
        table.createSqlTable(connection, namespace, true);
        String entityInsertSql = table.generateInsertSql(namespace, true);
        PreparedStatement insertStatement = connection.prepareStatement(entityInsertSql);
        // Iterate over agencies and add to prepared statement
        int count = 0, batchSize = 0;
        for (E entity : entities) {
            entity.setStatementParameters(insertStatement, true);
            insertStatement.addBatch();
            count++;
            batchSize++;
            // FIXME: Add batching execute on n
            if (batchSize > JdbcGtfsLoader.INSERT_BATCH_SIZE) {
                insertStatement.executeBatch();
                batchSize = 0;
            }
        }
        // Handle remaining
        insertStatement.executeBatch();
        LOG.info("Inserted {} {}", count, table.name);

        // FIXME: Should some tables not have indexes?
        table.createIndexes(connection, namespace);
    }
}
