package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.model.Calendar;
import com.conveyal.gtfs.model.CalendarDate;
import com.conveyal.gtfs.model.Entity;
import com.conveyal.gtfs.model.ScheduleException;
import com.conveyal.gtfs.model.Service;
import org.apache.commons.dbutils.DbUtils;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import javax.sql.DataSource;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Handles exporting a feed contained in the database to a GTFS zip file.
 */
public class JdbcGtfsExporter {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcGtfsExporter.class);

    private final String outFile;
    private final DataSource dataSource;
    private final boolean fromEditor;

    // These fields will be filled in once feed snapshot begins.
    private Connection connection;
    private ZipOutputStream zipOutputStream;
    // The reference feed ID (namespace) to copy.
    private final String feedIdToExport;

    public JdbcGtfsExporter(String feedId, String outFile, DataSource dataSource, boolean fromEditor) {
        this.feedIdToExport = feedId;
        this.outFile = outFile;
        this.dataSource = dataSource;
        this.fromEditor = fromEditor;
    }

    /**
     * Export primary entity tables as well as Pattern and PatternStops tables.
     *
     * FIXME: This should probably only export main GTFS tables... unless there is a benefit to keeping editor fields or
     * tables intact for future editing. Maybe this could be an option provided to the function (e.g., specTablesOnly).
     */
    public FeedLoadResult exportTables() {
        // This result object will be returned to the caller to summarize the feed and report any critical errors.
        // FIXME: use feed export result object?
        FeedLoadResult result = new FeedLoadResult();

        try {
            zipOutputStream = new ZipOutputStream(new FileOutputStream(outFile));
            long startTime = System.currentTimeMillis();
            // We get a single connection object and share it across several different methods.
            // This ensures that actions taken in one method are visible to all subsequent SQL statements.
            // If we create a schema or table on one connection, then access it in a separate connection, we have no
            // guarantee that it exists when the accessing statement is executed.
            connection = dataSource.getConnection();
            // Include the dot separator in the table prefix.
            // This allows everything to work even when there's no prefix.
            // Export each table in turn (by placing entry in zip output stream).
            // FIXME: NO non-fatal exception errors are being captured during copy operations.
            result.agency = export(Table.AGENCY);
            result.calendar = export(Table.CALENDAR);
            if (fromEditor) {
                // Export schedule exceptions in place of calendar dates if exporting from the GTFS Editor.
                GTFSFeed feed = new GTFSFeed();
                // FIXME: The below table readers should probably just share a connection with the exporter.
                JDBCTableReader<ScheduleException> exceptionsReader =
                        new JDBCTableReader(Table.SCHEDULE_EXCEPTIONS, dataSource, feedIdToExport + ".",
                                EntityPopulator.SCHEDULE_EXCEPTION);
                JDBCTableReader<Calendar> calendarsReader =
                        new JDBCTableReader(Table.CALENDAR, dataSource, feedIdToExport + ".",
                                EntityPopulator.CALENDAR);
                Iterable<Calendar> calendars = calendarsReader.getAll();
                for (Calendar cal : calendars) {
                    LOG.info("Iterating over calendar {}", cal.service_id);
                    Service service = new Service(cal.service_id);
                    service.calendar = cal;
                    Iterable<ScheduleException> exceptions = exceptionsReader.getAll();
                    for (ScheduleException ex : exceptions) {
                        LOG.info("Adding exception {} for calendar {}", ex.name, cal.service_id);
                        if (ex.equals(ScheduleException.ExemplarServiceDescriptor.SWAP) &&
                                !ex.addedService.contains(cal.service_id) && !ex.removedService.contains(cal.service_id))
                            // skip swap exception if cal is not referenced by added or removed service
                            // this is not technically necessary, but the output is cleaner/more intelligible
                            continue;

                        for (LocalDate date : ex.dates) {
                            if (date.isBefore(cal.start_date) || date.isAfter(cal.end_date))
                                // no need to write dates that do not apply
                                continue;

                            CalendarDate calendarDate = new CalendarDate();
                            calendarDate.date = date;
                            calendarDate.service_id = cal.service_id;
                            calendarDate.exception_type = ex.serviceRunsOn(cal) ? 1 : 2;

                            if (service.calendar_dates.containsKey(date))
                                throw new IllegalArgumentException("Duplicate schedule exceptions on " + date.toString());

                            service.calendar_dates.put(date, calendarDate);
                        }
                    }
                    feed.services.put(cal.service_id, service);
                }
                LOG.info("Writing calendar dates from schedule exceptions");
                new CalendarDate.Writer(feed).writeTable(zipOutputStream);
            } else {
                // Otherwise, simply export the calendar dates as they were loaded in.
                result.calendarDates = export(Table.CALENDAR_DATES);
            }
            result.fareAttributes = export(Table.FARE_ATTRIBUTES);
            result.fareRules = export(Table.FARE_RULES);
            result.feedInfo = export(Table.FEED_INFO);
            // FIXME: only write frequencies for "approved" routes using COPY TO with results of select query
            result.frequencies = export(Table.FREQUENCIES);
            // FIXME: only write "approved" routes using COPY TO with results of select query
            result.routes = export(Table.ROUTES);
            // FIXME: Find some place to store errors encountered on export for patterns and pattern stops.
            // FIXME: Is there a need to export patterns or pattern stops? Should these be iterated over to ensure that
            // frequency-based pattern travel times match stop time arrivals/departures?
//            export(Table.PATTERNS);
//            export(Table.PATTERN_STOP);
            // FIXME: only write shapes for "approved" routes using COPY TO with results of select query
            result.shapes = export(Table.SHAPES);
            result.stops = export(Table.STOPS);
            // FIXME: only write stop times for "approved" routes using COPY TO with results of select query
            result.stopTimes = export(Table.STOP_TIMES);
            result.transfers = export(Table.TRANSFERS);
            // FIXME: only write trips for "approved" routes using COPY TO with results of select query
            result.trips = export(Table.TRIPS);

            zipOutputStream.close();

            result.completionTime = System.currentTimeMillis();
            result.loadTimeMillis = result.completionTime - startTime;
            // Exporting primary GTFS tables for GRTA Xpress = 12 sec
            LOG.info("Exporting tables took {} sec", (result.loadTimeMillis) / 1000);
            LOG.info(outFile);
        } catch (Exception ex) {
            // Note: Exceptions that occur during individual table loads are separately caught and stored in
            // TableLoadResult.
            LOG.error("Exception while creating snapshot: {}", ex.toString());
            ex.printStackTrace();
            result.fatalException = ex.getMessage();
        }
        return result;
    }

    private TableLoadResult export (Table table) {
        TableLoadResult tableLoadResult = new TableLoadResult();
        try {
            // Use the Postgres text load format if we're connected to that DBMS.
            boolean postgresText = connection.getMetaData().getDatabaseProductName().equals("PostgreSQL");

            if (postgresText) {
                // Create entry for table
                ZipEntry zipEntry = new ZipEntry(table.name + ".txt");
                zipOutputStream.putNextEntry(zipEntry);

                // don't let CSVWriter close the stream when it is garbage-collected
                OutputStream protectedOut = new FilterOutputStream(zipOutputStream);
                String copySql = String.format("copy %s.%s to STDOUT DELIMITER ',' CSV HEADER", feedIdToExport, table.name);
                LOG.info(copySql);
                // Our connection pool wraps the Connection objects, so we need to unwrap the Postgres connection interface.
                CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
                copyManager.copyOut(copySql, protectedOut);
                zipOutputStream.closeEntry();
                LOG.info("Copy completed.");
            } else {
                LOG.error("Export not implemented for non-PostgreSQL databases.");
                throw new NotImplementedException();
            }
            connection.commit();
        } catch (SQLException e) {
            // Rollback connection so that fatal exception does not impact loading of other tables.
            try {
                connection.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            tableLoadResult.fatalException = e.getMessage();
            LOG.error("Exception while exporting tables", e);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return tableLoadResult;
    }
}
