package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.model.Calendar;
import com.conveyal.gtfs.model.CalendarDate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.conveyal.gtfs.loader.JdbcGtfsLoader.createFeedRegistryIfNotExists;
import static com.conveyal.gtfs.loader.JdbcGtfsLoader.createSchema;
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
    /**
     * Whether to normalize stop_times#stop_sequence values on snapshot (or leave them intact).
     *
     * TODO: if more options are added in the future, this should be folded into a SnapshotOptions
     *   object.
     */
    private final boolean normalizeStopTimes;

    // These fields will be filled in once feed snapshot begins.
    private Connection connection;
    private String tablePrefix;
    // The reference feed ID (namespace) to copy.
    private final String feedIdToSnapshot;

    /**
     * @param feedId namespace (schema) to snapshot. If null, a blank snapshot will be created.
     * @param dataSource the JDBC data source with database connection details
     * @param normalizeStopTimes whether to keep stop sequence values intact or normalize to be zero-based and
     *                           incrementing
     */
    public JdbcGtfsSnapshotter(String feedId, DataSource dataSource, boolean normalizeStopTimes) {
        this.feedIdToSnapshot = feedId;
        this.dataSource = dataSource;
        this.normalizeStopTimes = normalizeStopTimes;
    }

    /**
     * Copy primary entity tables as well as Pattern and PatternStops tables.
     */
    public SnapshotResult copyTables() {
        // This result object will be returned to the caller to summarize the feed and report any critical errors.
        SnapshotResult result = new SnapshotResult();

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
            result.bookingRules = copy(Table.BOOKING_RULES, true);
            result.calendarDates = copy(Table.CALENDAR_DATES, true);
            result.fareAttributes = copy(Table.FARE_ATTRIBUTES, true);
            result.fareRules = copy(Table.FARE_RULES, true);
            result.feedInfo = copy(Table.FEED_INFO, true);
            result.frequencies = copy(Table.FREQUENCIES, true);
            result.locationGroups = copy(Table.LOCATION_GROUPS, true);
            result.routes = copy(Table.ROUTES, true);
            // FIXME: Find some place to store errors encountered on copy for patterns and pattern stops.
            copy(Table.PATTERNS, true);
            copy(Table.PATTERN_STOP, true);
            // see method comments fo why different logic is needed for this table
            result.scheduleExceptions = createScheduleExceptionsTable();
            result.shapes = copy(Table.SHAPES, true);
            result.stops = copy(Table.STOPS, true);
            // TODO: Should we defer index creation on stop times?
            // Copying all tables for STIF w/ stop times idx = 156 sec; w/o = 28 sec
            // Other feeds w/ stop times AC Transit = 3 sec; Brooklyn bus =
            result.stopTimes = copy(Table.STOP_TIMES, true);
            result.transfers = copy(Table.TRANSFERS, true);
            result.trips = copy(Table.TRIPS, true);
            result.attributions = copy(Table.ATTRIBUTIONS, true);
            result.translations = copy(Table.TRANSLATIONS, true);
            result.bookingRules = copy(Table.BOOKING_RULES, true);
            result.locationGroups = copy(Table.LOCATION_GROUPS, true);
            result.completionTime = System.currentTimeMillis();
            result.loadTimeMillis = result.completionTime - startTime;
            LOG.info("Copying tables took {} sec", (result.loadTimeMillis) / 1000);
        } catch (Exception ex) {
            // Note: Exceptions that occur during individual table loads are separately caught and stored in
            // TableLoadResult.
            LOG.error("Exception while creating snapshot: {}", ex.toString());
            ex.printStackTrace();
            result.fatalException = ex.toString();
        } finally {
            if (connection != null) DbUtils.closeQuietly(connection);
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
                success = targetTable.createSqlTableFrom(connection, fromTableName, normalizeStopTimes);
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
            tableLoadResult.fatalException = ex.toString();
            LOG.error("Error: ", ex);
            try {
                connection.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return tableLoadResult;
    }

    /**
     * Special logic is needed for creating the schedule_exceptions table.
     *
     * gtfs-lib has some more advanced data types in addition to those available in the calendar_dates.txt file of the
     * GTFS specification.  The schedule_exceptions table is the source of truth for exceptions to regular schedules.
     * When exporting to a GTFS, the calendar_dates table is completely ignored if the schedule_exceptions table exists.
     *
     * When creating a snapshot, if the schedule_exceptions table doesn't currently exist, it is assumed that the feed
     * being copied has just been imported and additional data to explain schedule_exceptions has not been generated yet.
     * If the schedule_exceptions does already exist, that table is simply copied over.
     *
     * If the calendar table does not exist in the feed being copied from, it might have been the case that the
     * imported feed did not have a calendar.txt file. If that was the case, then the schedule exceptions need to be
     * generated from the calendar_dates table and also the calendar table needs to be populated with dummy entries so
     * that it is possible to export the data to a GTFS because of current logic in JdbcGtfsExporter.  The way
     * JdbcGtfsExporter will only export calendar_dates that have a corresponding entry in the calendar table and have a
     * service span that applies to a schedule_exception.  Furthermore, the dummy calendar entries are currently needed
     * for the downstream library datatools-server/datatools-ui to work properly.
     */
    private TableLoadResult createScheduleExceptionsTable() {
        // check to see if the schedule_exceptions table exists
        boolean scheduleExceptionsTableExists = tableExists(feedIdToSnapshot, "schedule_exceptions");
        String scheduleExceptionsTableName = tablePrefix + "schedule_exceptions";

        if (scheduleExceptionsTableExists) {
            // schedule_exceptions table already exists in namespace being copied from.  Therefore, we simply copy it.
            return copy(Table.SCHEDULE_EXCEPTIONS, true);
        } else {
            // schedule_exceptions does not exist.  Therefore, we generate schedule_exceptions from the calendar_dates.
            TableLoadResult tableLoadResult = new TableLoadResult();
            try {
                Table.SCHEDULE_EXCEPTIONS.createSqlTable(
                    connection,
                    tablePrefix.replace(".", ""),
                    true
                );
                String sql = String.format(
                    "insert into %s (name, dates, exemplar, added_service, removed_service) values (?, ?, ?, ?, ?)",
                    scheduleExceptionsTableName
                );
                PreparedStatement scheduleExceptionsStatement = connection.prepareStatement(sql);
                final BatchTracker scheduleExceptionsTracker = new BatchTracker(
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

                // Keep track of calendars by service id in case we need to add dummy calendar entries.
                Map<String, Calendar> dummyCalendarsByServiceId = new HashMap<>();

                // Iterate through calendar dates to build up to get maps from exceptions to their dates.
                Multimap<String, String> removedServiceForDate = HashMultimap.create();
                Multimap<String, String> addedServiceForDate = HashMultimap.create();
                for (CalendarDate calendarDate : calendarDates) {
                    // Skip any null dates
                    if (calendarDate.date == null) {
                        LOG.warn("Encountered calendar date record with null value for date field. Skipping.");
                        continue;
                    }
                    String date = calendarDate.date.format(DateTimeFormatter.BASIC_ISO_DATE);
                    if (calendarDate.exception_type == 1) {
                        addedServiceForDate.put(date, calendarDate.service_id);
                        // create (if needed) and extend range of dummy calendar that would need to be created if we are
                        // copying from a feed that doesn't have the calendar.txt file
                        Calendar calendar = dummyCalendarsByServiceId.getOrDefault(calendarDate.service_id, new Calendar());
                        calendar.service_id = calendarDate.service_id;
                        if (calendar.start_date == null || calendar.start_date.isAfter(calendarDate.date)) {
                            calendar.start_date = calendarDate.date;
                        }
                        if (calendar.end_date == null || calendar.end_date.isBefore(calendarDate.date)) {
                            calendar.end_date = calendarDate.date;
                        }
                        dummyCalendarsByServiceId.put(calendarDate.service_id, calendar);
                    } else {
                        removedServiceForDate.put(date, calendarDate.service_id);
                    }
                }
                // Iterate through dates with added or removed service and add to database.
                // For usability and simplicity of code, don't attempt to find all dates with similar
                // added and removed services, but simply create an entry for each found date.
                for (String date : Sets.union(removedServiceForDate.keySet(), addedServiceForDate.keySet())) {
                    scheduleExceptionsStatement.setString(1, date);
                    String[] dates = {date};
                    scheduleExceptionsStatement.setArray(2, connection.createArrayOf("text", dates));
                    scheduleExceptionsStatement.setInt(3, 9); // FIXME use better static type
                    scheduleExceptionsStatement.setArray(
                        4,
                        connection.createArrayOf("text", addedServiceForDate.get(date).toArray())
                    );
                    scheduleExceptionsStatement.setArray(
                        5,
                        connection.createArrayOf("text", removedServiceForDate.get(date).toArray())
                    );
                    scheduleExceptionsTracker.addBatch();
                }
                scheduleExceptionsTracker.executeRemaining();

                // fetch all entries in the calendar table to generate set of serviceIds that exist in the calendar
                // table.
                JDBCTableReader<Calendar> calendarReader = new JDBCTableReader(
                    Table.CALENDAR,
                    dataSource,
                    feedIdToSnapshot + ".",
                    EntityPopulator.CALENDAR
                );
                Set<String> calendarServiceIds = new HashSet<>();
                for (Calendar calendar : calendarReader.getAll()) {
                    calendarServiceIds.add(calendar.service_id);
                }

                // For service_ids that only existed in the calendar_dates table, insert auto-generated, "blank"
                // (no days of week specified) calendar entries.
                sql = String.format(
                    "insert into %s (service_id, description, start_date, end_date, " +
                        "monday, tuesday, wednesday, thursday, friday, saturday, sunday)" +
                        "values (?, ?, ?, ?, 0, 0, 0, 0, 0, 0, 0)",
                    tablePrefix + "calendar"
                );
                PreparedStatement calendarStatement = connection.prepareStatement(sql);
                final BatchTracker calendarsTracker = new BatchTracker(
                    "calendar",
                    calendarStatement
                );
                for (Calendar dummyCalendar : dummyCalendarsByServiceId.values()) {
                    if (calendarServiceIds.contains(dummyCalendar.service_id)) {
                        // This service_id already exists in the calendar table. No need to create auto-generated entry.
                        continue;
                    }
                    calendarStatement.setString(1, dummyCalendar.service_id);
                    calendarStatement.setString(
                        2,
                        String.format("%s (auto-generated)", dummyCalendar.service_id)
                    );
                    calendarStatement.setString(
                        3,
                        dummyCalendar.start_date.format(DateTimeFormatter.BASIC_ISO_DATE)
                    );
                    calendarStatement.setString(
                        4,
                        dummyCalendar.end_date.format(DateTimeFormatter.BASIC_ISO_DATE)
                    );
                    calendarsTracker.addBatch();
                }
                calendarsTracker.executeRemaining();

                connection.commit();
            } catch (Exception e) {
                tableLoadResult.fatalException = e.toString();
                LOG.error("Error creating schedule Exceptions: ", e);
                e.printStackTrace();
                try {
                    connection.rollback();
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
            LOG.info("done creating schedule exceptions");
            return tableLoadResult;
        }
    }

    /**
     * Helper method to determine if a table exists within a namespace.
     */
    private boolean tableExists(String namespace, String tableName) {
        // Preempt SQL check with null check of either namespace or table name.
        if (namespace == null || tableName == null) return false;
        try {
            // This statement is postgres-specific.
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
     * This method contains a SQL statement that requires PostgreSQL 9.6+.
     */
    private void addEditorSpecificFields(Connection connection, String tablePrefix, Table table) throws SQLException {
        LOG.info("Adding any missing columns for {}", tablePrefix + table.name);
        Statement statement = connection.createStatement();
        for (Field field : table.editorFields()) {
            // The following statement requires PostgreSQL 9.6+.
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
            // We cannot simply insert into the feeds table because if we are creating an empty snapshot (to create/edit
            // a GTFS feed from scratch), the feed registry table will not exist.
            // TODO copy over feed_id and feed_version from source namespace?
            // TODO: Record total snapshot processing time?
            createFeedRegistryIfNotExists(connection);
            createSchema(connection, tablePrefix);
            PreparedStatement insertStatement = connection.prepareStatement(
                    "insert into feeds values (?, null, null, null, null, null, current_timestamp, ?, false)");
            insertStatement.setString(1, tablePrefix);
            insertStatement.setString(2, feedIdToSnapshot);
            insertStatement.execute();
            connection.commit();
            LOG.info("Created new snapshot namespace: {}", insertStatement);
        } catch (Exception ex) {
            LOG.error("Exception while registering snapshot namespace in feeds table", ex);
            DbUtils.closeQuietly(connection);
        }
    }
}
