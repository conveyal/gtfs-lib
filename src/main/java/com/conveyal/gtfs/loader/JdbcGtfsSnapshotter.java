package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.model.Calendar;
import com.conveyal.gtfs.model.CalendarDate;
import com.conveyal.gtfs.validator.ServiceValidator;
import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import static com.conveyal.gtfs.util.Util.randomIdString;

/**
 * This class takes a feedId that represents a feed already in the database and creates a copy of the entire feed.
 * All tables except for the derived error and service tables are copied over (the derived pattern and pattern stop
 * tables ARE copied over).
 *
 * This copy functionality is intended to make the feed editable and so the resulting feed
 * tables are somewhat modified from their original read-only source. For instance, the ID column has been modified
 * so that it is an auto-incrementing serial integer, changing the meaning of the column from csv_line (for feeds
 * loaded from GTFS) to a unique identifier used to reference entities in an API.
 */
public class JdbcGtfsSnapshotter {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcGtfsSnapshotter.class);

    private final DataSource dataSource;

    // These fields will be filled in once feed snapshot begins.
    private Connection connection;
    private String tablePrefix;
    // The reference feed ID (namespace) to copy.
    private final String feedIdToSnapshot;

    /**
     * @param feedId namespace (schema) to snapshot. If null, a blank snapshot will be created.
     * @param dataSource the JDBC data source with database connection details
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
            long startTime = System.currentTimeMillis();
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
            // Copy each table in turn
            // FIXME: NO non-fatal exception errors are being captured during copy operations.
            result.agency = copy(Table.AGENCY, true);
            result.calendar = copy(Table.CALENDAR, true);
            result.calendarDates = copy(Table.CALENDAR_DATES, true);
            result.fareAttributes = copy(Table.FARE_ATTRIBUTES, true);
            result.fareRules = copy(Table.FARE_RULES, true);
            result.feedInfo = copy(Table.FEED_INFO, true);
            result.frequencies = copy(Table.FREQUENCIES, true);
            result.routes = copy(Table.ROUTES, true);
            // FIXME: Find some place to store errors encountered on copy for patterns and pattern stops.
            copy(Table.PATTERNS, true);
            copy(Table.PATTERN_STOP, true);
            createScheduleExceptionsTable();
            result.shapes = copy(Table.SHAPES, true);
            result.stops = copy(Table.STOPS, true);
            // TODO: Should we defer index creation on stop times?
            // Copying all tables for STIF w/ stop times idx = 156 sec; w/o = 28 sec
            // Other feeds w/ stop times AC Transit = 3 sec; Brooklyn bus =
            result.stopTimes = copy(Table.STOP_TIMES, true);
            result.transfers = copy(Table.TRANSFERS, true);
            result.trips = copy(Table.TRIPS, true);
            result.completionTime = System.currentTimeMillis();
            result.loadTimeMillis = result.completionTime - startTime;
            LOG.info("Copying tables took {} sec", (result.loadTimeMillis) / 1000);
        } catch (Exception ex) {
            // Note: Exceptions that occur during individual table loads are separately caught and stored in
            // TableLoadResult.
            LOG.error("Exception while creating snapshot: {}", ex.toString());
            ex.printStackTrace();
            result.fatalException = ex.getMessage();
        }
        return result;
    }

    /**
     * This is the main table copy method that wraps a call to Table#createSqlTableFrom and creates indexes for
     * the table.
     */
    private TableLoadResult copy (Table table, boolean createIndexes) {
        // This object will be returned to the caller to summarize the contents of the table and any errors.
        // FIXME: Should there be a separate TableSnapshotResult? Load result is empty except for fatal exception.
        TableLoadResult tableLoadResult = new TableLoadResult();
        try {
            // FIXME this is confusing, we only create a new table object so we can call a couple of methods on it,
            // all of which just need a list of fields.
            Table targetTable = new Table(tablePrefix + table.name, table.entityClass, table.required, table.fields);
            boolean success;
            if (feedIdToSnapshot == null) {
                // If there is no feedId to snapshot (i.e., we're making an empty snapshot), simply create the table.
                success = targetTable.createSqlTable(connection, true);
            } else {
                // Otherwise, use the createTableFrom method to copy the data from the original.
                String fromTableName = String.format("%s.%s", feedIdToSnapshot, table.name);
                LOG.info("Copying table {} to {}", fromTableName, targetTable.name);
                success = targetTable.createSqlTableFrom(connection, fromTableName);
            }
            // Only create indexes if table creation was successful.
            if (success && createIndexes) {
                addEditorSpecificFields(connection, tablePrefix, table);
                // Use spec table to create indexes. See createIndexes method for more info on why.
                table.createIndexes(connection, tablePrefix);
                // Populate default values for editor fields, including normalization of stop time stop sequences.
                populateDefaultEditorValues(connection, tablePrefix, table);
            }
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
     * Special logic is needed for creating the schedule_exceptions table.
     * If the calendar_dates table does not exist in the feed being copied from, it might have been the case that the
     * imported feed did not have a calendars.txt file. If that was the case, then the schedule exceptions need to be
     * generated from the calendar_dates table and also the calendars table needs to be populated with dummy entries so
     * that it is possible to export the data to a GTFS because of current logic in JdbcGtfsExporter.  This all may
     * change in a future iteration of development.
     */
    private TableLoadResult createScheduleExceptionsTable() {
        // check to see if the schedule_exceptions table exists
        boolean scheduleExceptionsTableExists;
        String scheduleExceptionsTableName = tablePrefix + "schedule_exceptions";
        if (feedIdToSnapshot == null) {
            scheduleExceptionsTableExists = false;
        } else {
            scheduleExceptionsTableExists = tableExists(feedIdToSnapshot, "schedule_exceptions");
        }

        if (scheduleExceptionsTableExists) {
            // schedule_exceptions table already exists in namespace being copied from.  Therefore, we simply copy it.
            return copy(Table.SCHEDULE_EXCEPTIONS, true);
        } else {
            // schedule_exceptions does not exist.  Therefore, we generate schedule_exceptions from the calendar_dates.
            TableLoadResult tableLoadResult = new TableLoadResult();
            try {
                String sql = String.format(
                    "create table %s (id serial, name varchar, dates text[], exemplar smallint," +
                        "custom_schedule text[], added_service text[], removed_service text[])",
                    scheduleExceptionsTableName
                );
                LOG.info(sql);
                Statement statement = connection.createStatement();
                statement.execute(sql);
                sql = String.format(
                    "insert into %s (name, dates, exemplar, added_service, removed_service)" +
                        "values (?, ?, ?, ?, ?)",
                    scheduleExceptionsTableName
                );
                PreparedStatement scheduleExceptionsStatement = connection.prepareStatement(sql);
                final ServiceValidator.BatchTracker scheduleExceptionsTracker = new ServiceValidator.BatchTracker(
                    "schedule_exceptions",
                    scheduleExceptionsStatement
                );

                JDBCTableReader<CalendarDate> calendarDatesReader = new JDBCTableReader(
                    Table.CALENDAR_DATES,
                    dataSource,
                    feedIdToSnapshot + ".",
                    EntityPopulator.CALENDAR_DATE
                );
                Iterable<CalendarDate> calendarDates = calendarDatesReader.getAll();

                // keep track of calendars by service id in case we need to add dummy calendar entries
                HashMap<String, Calendar> calendarsByServiceId = new HashMap<>();

                // iterate through all calendarDates and add a corresponding entry into schedule_exceptions on a 1-1
                // basis to make things a straightforward as possible
                for (CalendarDate calendarDate : calendarDates) {
                    String dateWithException = calendarDate.date.format(DateTimeFormatter.BASIC_ISO_DATE);
                    Array empty = connection.createArrayOf("text", new ArrayList<>().toArray());
                    Array service = connection.createArrayOf("text", new String[]{calendarDate.service_id});

                    scheduleExceptionsStatement.setString(1, dateWithException);
                    scheduleExceptionsStatement.setArray(
                        2,
                        connection.createArrayOf("text", new String[]{dateWithException})
                    );
                    scheduleExceptionsStatement.setInt(3, 9); // FIXME use better static type
                    if (calendarDate.exception_type == 1) {
                        scheduleExceptionsStatement.setArray(4, service);
                        scheduleExceptionsStatement.setArray(5, empty);
                        // create (if needed) and extend range of dummy calendar that would need to be created if we are
                        // copying from a feed that doesn't have the calendar.txt file
                        Calendar calendar = calendarsByServiceId.getOrDefault(calendarDate.service_id, new Calendar());
                        calendar.service_id = calendarDate.service_id;
                        if (calendar.start_date == null || calendar.start_date.isAfter(calendarDate.date)) {
                            calendar.start_date = calendarDate.date;
                        }
                        if (calendar.end_date == null || calendar.end_date.isBefore(calendarDate.date)) {
                            calendar.end_date = calendarDate.date;
                        }
                        calendarsByServiceId.put(calendarDate.service_id, calendar);
                    } else {
                        scheduleExceptionsStatement.setArray(4, empty);
                        scheduleExceptionsStatement.setArray(5, service);
                    }
                    scheduleExceptionsTracker.addBatch();
                }
                scheduleExceptionsTracker.executeRemaining();

                // determine if we appear to be working with a calendar_dates-only feed.
                // If so, we must also add dummy entries to the calendar table
                if (!tableExists(feedIdToSnapshot, "calendar") && calendarDatesReader.getRowCount() > 0) {
                    sql = String.format(
                        "insert into %s (service_id, start_date, end_date)" +
                            "values (?, ?, ?)",
                        tablePrefix + "calendar"
                    );
                    PreparedStatement calendarStatement = connection.prepareStatement(sql);
                    final ServiceValidator.BatchTracker calendarsTracker = new ServiceValidator.BatchTracker(
                        "calendar",
                        calendarStatement
                    );
                    for (Calendar calendar : calendarsByServiceId.values()) {
                        calendarStatement.setString(1, calendar.service_id);
                        calendarStatement.setString(
                            2,
                            calendar.start_date.format(DateTimeFormatter.BASIC_ISO_DATE)
                        );
                        calendarStatement.setString(
                            3,
                            calendar.end_date.format(DateTimeFormatter.BASIC_ISO_DATE)
                        );
                        calendarsTracker.addBatch();
                    }
                    calendarsTracker.executeRemaining();
                }

                connection.commit();
            } catch (SQLException e) {
                tableLoadResult.fatalException = e.getMessage();
                LOG.error("Error creating schedule Exceptions: ", e);
                e.printStackTrace();
            }
            LOG.info("done creating schedule exceptions");
            return tableLoadResult;
        }
    }

    /**
     * Helper method to determine if a table exists within a namespace.
     * FIXME This is postgres-specific and needs to be made generic for non-postgres databases.
     */
    private boolean tableExists(String namespace, String tableName) {
        try {
            PreparedStatement tableExistsStatement = connection.prepareStatement(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?)"
            );
            tableExistsStatement.setString(1, namespace);
            tableExistsStatement.setString(2, tableName);
            ResultSet resultSet = tableExistsStatement.executeQuery();
            resultSet.next();
            return resultSet.getBoolean(1);
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Add columns for any required, optional, or editor fields that don't already exist as columns on the table.
     */
    private void addEditorSpecificFields(Connection connection, String tablePrefix, Table table) throws SQLException {
        LOG.info("Adding any missing columns for {}", tablePrefix + table.name);
        Statement statement = connection.createStatement();
        for (Field field : table.editorFields()) {
            String addColumnSql = String.format("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s",
                    tablePrefix + table.name,
                    field.name,
                    field.getSqlTypeName());
            LOG.info(addColumnSql);
            statement.execute(addColumnSql);
        }
    }

    /**
     * Populates editor-specific fields added during GTFS-to-snapshot operation with default values. This method also
     * "normalizes" the stop sequences for stop times to zero-based, incremented integers. NOTE: stop time normalization
     * can take a significant amount of time (for large feeds up to 5 minutes) if the update is large.
     */
    private void populateDefaultEditorValues(Connection connection, String tablePrefix, Table table) throws SQLException {
        Statement statement = connection.createStatement();
        if (Table.ROUTES.name.equals(table.name)) {
            // Set default values for route status and publicly visible to "Approved" and "Public", respectively.
            // This prevents unexpected results when users attempt to export a GTFS feed from the editor and no
            // routes are exported due to undefined values for status and publicly visible.
            String updateStatusSql = String.format(
                    "update %sroutes set status = 2, publicly_visible = 1 where status is NULL AND publicly_visible is NULL",
                    tablePrefix);
            int updatedRoutes = statement.executeUpdate(updateStatusSql);
            LOG.info("Updated status for {} routes", updatedRoutes);
        }
        if (Table.CALENDAR.name.equals(table.name)) {
            // Set default values for description field. Basically constructs a description from the days of the week
            // for which the calendar is active.
            LOG.info("Updating calendar descriptions");
            String[] daysOfWeek = new String[]{"monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"};
            String concatenatedDaysOfWeek =  String.join(", ",
                    Arrays.stream(daysOfWeek)
                        .map(d -> String.format(
                                    "case %s when 1 then '%s' else '' end",
                                    d,
                                // Capitalize first letter. Converts days of week from "monday" -> "Mo".
                                d.substring(0, 1).toUpperCase() + d.substring(1, 2))).toArray(String[]::new));
            String updateOtherSql = String.format(
                    "update %scalendar set description = concat(%s) where description is NULL",
                    tablePrefix,
                    concatenatedDaysOfWeek);
            LOG.info(updateOtherSql);
            int calendarsUpdated = statement.executeUpdate(updateOtherSql);
            LOG.info("Updated description for {} calendars", calendarsUpdated);
        }
        if (Table.TRIPS.name.equals(table.name)) {
            // Update use_frequency field for patterns. This sets all patterns that have a frequency trip to use
            // frequencies. NOTE: This is performed after copying the TRIPS table rather than after PATTERNS because
            // both tables (plus, frequencies) need to exist for the successful operation.
            // TODO: How should this handle patterns that have both timetable- and frequency-based trips?
            // NOTE: The below substitution uses argument indexing. All values "%1$s" reference the first argument
            // supplied (i.e., tablePrefix).
            String updatePatternsSql = String.format(
                    "update %1$spatterns set use_frequency = 1 " +
                    "from (select distinct %1$strips.pattern_id from %1$strips, %1$sfrequencies where %1$sfrequencies.trip_id = %1$strips.trip_id) freq " +
                    "where freq.pattern_id = %1$spatterns.pattern_id",
                    tablePrefix);
            LOG.info(updatePatternsSql);
            int patternsUpdated = statement.executeUpdate(updatePatternsSql);
            LOG.info("Updated use_frequency for {} patterns", patternsUpdated);
        }
        // TODO: Add simple conversion from calendar_dates to schedule_exceptions if no exceptions exist? See
        // https://github.com/catalogueglobal/datatools-server/issues/80
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
            LOG.error("Exception while registering snapshot namespace in feeds table: {}", ex.getMessage());
            DbUtils.closeQuietly(connection);
        }
    }
}
