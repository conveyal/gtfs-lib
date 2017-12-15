package com.conveyal.gtfs.loader;

import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

import static com.conveyal.gtfs.util.Util.randomIdString;

public class JdbcGtfsSnapshotter {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcGtfsSnapshotter.class);

    private final DataSource dataSource;

    // These fields will be filled in once feed snapshot begins.
    private Connection connection;
    private String tablePrefix;
    // The reference feed ID (namespace) to copy.
    private final String feedIdToSnapshot;

    /**
     * This class takes a feedId that represents a feed already in the database and creates a copy of the entire feed.
     * All tables except for the derived error and service tables are copied over (the derived pattern and pattern stop
     * tables ARE copied over).
     *
     * This copy functionality is intended to make the feed editable and so the resulting feed
     * tables are somewhat modified from their original read-only source. For instance, the ID column has been modified
     * so that it is an auto-incrementing serial integer, changing the meaning of the column from csv_line (for feeds
     * loaded from GTFS) to a unique identifier used to reference entities in an API.
     * @param feedId the reference feed ID (namespace) to copy
     */
    public JdbcGtfsSnapshotter(String feedId, DataSource dataSource) {
        this.feedIdToSnapshot = feedId;
        this.dataSource = dataSource;
    }

    /**
     * Copy primary entity tables as well as Pattern and PatternStops tables.
     */
    public FeedLoadResult copyTables() {
        // This result object will be returned to the caller to summarize the feed and report any critical errors.
        FeedLoadResult result = new FeedLoadResult();

        try {
            // We get a single connection object and share it across several different methods.
            // This ensures that actions taken in one method are visible to all subsequent SQL statements.
            // If we create a schema or table on one connection, then access it in a separate connection, we have no
            // guarantee that it exists when the accessing statement is executed.
            connection = dataSource.getConnection();
            // Generate a unique prefix that will identify this feed.
            this.tablePrefix = randomIdString();
            result.uniqueIdentifier = tablePrefix;
            // Create entry in snapshots table.
            registerSnapshot();
            // Include the dot separator in the table prefix.
            // This allows everything to work even when there's no prefix.
            this.tablePrefix += ".";
            long startTime = System.currentTimeMillis();
            // Copy each table in turn
            result.agency = copy(Table.AGENCY, true, true);
            result.calendar = copy(Table.CALENDAR, true, true);
            result.calendarDates = copy(Table.CALENDAR_DATES, true, false);
            result.fareAttributes = copy(Table.FARE_ATTRIBUTES, true, true);
            result.fareRules = copy(Table.FARE_RULES, true, true);
            result.feedInfo = copy(Table.FEED_INFO, true, false);
            result.frequencies = copy(Table.FREQUENCIES, true, true);
            result.routes = copy(Table.ROUTES, true, true);
            copy(Table.PATTERNS, true, true);
            copy(Table.PATTERN_STOP, true, true);
            result.shapes = copy(Table.SHAPES, true, false);
            result.stops = copy(Table.STOPS, true, true);
            // TODO: Should we defer index creation on stop times?
            // Copying all tables for STIF w/ stop times idx = 156 sec; w/o = 28 sec
            // Other feeds w/ stop times AC Transit = 3 sec; Brooklyn bus =
            result.stopTimes = copy(Table.STOP_TIMES, true, false);
            result.transfers = copy(Table.TRANSFERS, true, true);
            result.trips = copy(Table.TRIPS, true, true);
            LOG.info("Copying tables took {} sec", (System.currentTimeMillis() - startTime) / 1000);
        } catch (Exception ex) {
            // TODO catch exceptions separately while loading each table so load can continue, store in TableLoadResult
            LOG.error("Exception while creating snapshot: {}", ex.toString());
            ex.printStackTrace();
            result.fatalException = ex.getMessage();
        }
        return result;
    }

    /**
     * This is the main table copy method that encapsulates a call to Table#createSqlTableFrom and creates indexes for
     * the table.
     */
    private TableLoadResult copy (Table table, boolean createIndexes, boolean addPrimaryKey) {
        // This object will be returned to the caller to summarize the contents of the table and any errors.
        // FIXME: Should there be a separate TableSnapshotResult? Load result is empty except for fatal exception.
        TableLoadResult tableLoadResult = new TableLoadResult();
        try {
            Table targetTable = new Table(tablePrefix + table.name, table.entityClass, table.required, table.fields);
            String fromTableName = String.format("%s.%s", feedIdToSnapshot, table.name);
            LOG.info("Copying table {} to {}", fromTableName, targetTable.name);
            boolean success = targetTable.createSqlTableFrom(connection, fromTableName, addPrimaryKey);
            // Only create indexes if table clone was successful.
            if (success && createIndexes) targetTable.createIndexes(connection);
            LOG.info("Committing transaction...");
            connection.commit();
            LOG.info("Done.");
        } catch (Exception ex) {
            tableLoadResult.fatalException = ex.getMessage();
            LOG.error("Error: ", ex);
        }
        return tableLoadResult;
    }

    /**
     * Add a line to the list of loaded feeds to record the snapshot and which feed the snapshot replicates.
     */
    private void registerSnapshot () {
        try {
            Statement statement = connection.createStatement();
            // TODO copy over feed_id and feed_version from source namespace?

            // FIXME do the following only on databases that support schemas.
            // SQLite does not support them. Is there any advantage of schemas over flat tables?
            statement.execute("create schema " + tablePrefix);
            // TODO load more stuff from feed_info and essentially flatten all feed_infos from all loaded feeds into one table
            // TODO: Record total snapshot processing time?
            // Simply insert into feeds table (no need for table creation) because making a snapshot presumes that the
            // feeds table already exists.
            PreparedStatement insertStatement = connection.prepareStatement(
                    "insert into feeds values (?, null, null, null, null, null, current_timestamp, ?)");
            insertStatement.setString(1, tablePrefix);
            insertStatement.setString(2, feedIdToSnapshot);
            insertStatement.execute();
            connection.commit();
            LOG.info("Created new snapshot namespace: {}", insertStatement);
        } catch (Exception ex) {
            LOG.error("Exception while creating unique prefix for new feed: {}", ex.getMessage());
            DbUtils.closeQuietly(connection);
        }
    }
}
