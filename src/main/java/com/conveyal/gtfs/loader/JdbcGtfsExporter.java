package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.model.Calendar;
import com.conveyal.gtfs.model.CalendarDate;
import com.conveyal.gtfs.model.LocationMetaData;
import com.conveyal.gtfs.model.LocationShape;
import com.conveyal.gtfs.model.ScheduleException;
import com.conveyal.gtfs.model.Service;
import com.conveyal.gtfs.util.GeoJsonException;
import com.conveyal.gtfs.util.GeoJsonUtil;
import com.google.common.collect.Lists;
import org.apache.commons.dbutils.DbUtils;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private List<String> emptyTableList = new ArrayList<>();

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
            if (!connection.getMetaData().getDatabaseProductName().equals("PostgreSQL")) {
                // This code path currently requires the Postgres text "copy to" format.
                throw new RuntimeException("Export from SQL database not implemented for non-PostgreSQL databases.");
            }
            // Construct where clause for routes table to filter only "approved" routes (and entities related to routes)
            // if exporting a feed/schema that represents an editor snapshot.
            String whereRouteIsApproved = String.format("where %s.%s.status = 2", feedIdToExport, Table.ROUTES.name);
            // Export each table in turn (by placing entry in zip output stream).
            result.agency = export(Table.AGENCY, connection);
            // TODO: No idea if booking rules and location groups need to have a 'fromEditor' option?
            result.bookingRules = export(Table.BOOKING_RULES, connection);
            result.locationGroups = export(Table.LOCATION_GROUPS, connection);
            if (fromEditor) {
                // only export calendar entries that have at least one day of service set
                // this could happen in cases where a feed was imported that only had calendar_dates.txt
                result.calendar = export(
                    Table.CALENDAR,
                    String.join(
                        " ",
                        Table.CALENDAR.generateSelectSql(feedIdToExport, Requirement.OPTIONAL),
                        "WHERE monday=1 OR tuesday=1 OR wednesday=1 OR thursday=1 OR friday=1 OR saturday=1 OR sunday=1"
                    )
                );
            } else {
                result.calendar = export(Table.CALENDAR, connection);
            }
            if (fromEditor) {
                // Export schedule exceptions in place of calendar dates if exporting a feed/schema that represents an editor snapshot.
                GTFSFeed feed = new GTFSFeed();
                // FIXME: The below table readers should probably just share a connection with the exporter.
                JDBCTableReader<ScheduleException> exceptionsReader =
                        new JDBCTableReader(Table.SCHEDULE_EXCEPTIONS, dataSource, feedIdToExport + ".",
                                EntityPopulator.SCHEDULE_EXCEPTION);
                JDBCTableReader<Calendar> calendarsReader =
                        new JDBCTableReader(Table.CALENDAR, dataSource, feedIdToExport + ".",
                                EntityPopulator.CALENDAR);
                Iterable<Calendar> calendars = calendarsReader.getAll();
                Iterable<ScheduleException> exceptionsIterator = exceptionsReader.getAll();
                List<ScheduleException> exceptions = new ArrayList<>();
                // FIXME: Doing this causes the connection to stay open, but it is closed in the finalizer so it should
                // not be a big problem.
                for (ScheduleException exception : exceptionsIterator) {
                    exceptions.add(exception);
                }
                // check whether the feed is organized in a format with the calendars.txt file
                if (calendarsReader.getRowCount() > 0) {
                    // feed does have calendars.txt file, continue export with strategy of matching exceptions
                    // to calendar to output calendar_dates.txt
                    int calendarDateCount = 0;
                    for (Calendar cal : calendars) {
                        Service service = new Service(cal.service_id);
                        service.calendar = cal;
                        for (ScheduleException ex : exceptions) {
                            if (ex.exemplar.equals(ScheduleException.ExemplarServiceDescriptor.SWAP) &&
                                (!ex.addedService.contains(cal.service_id) && !ex.removedService.contains(cal.service_id))) {
                                // Skip swap exception if cal is not referenced by added or removed service.
                                // This is not technically necessary, but the output is cleaner/more intelligible.
                                continue;
                            }

                            for (LocalDate date : ex.dates) {
                                if (date.isBefore(cal.start_date) || date.isAfter(cal.end_date)) {
                                    // No need to write dates that do not apply
                                    continue;
                                }

                                CalendarDate calendarDate = new CalendarDate();
                                calendarDate.date = date;
                                calendarDate.service_id = cal.service_id;
                                calendarDate.exception_type = ex.serviceRunsOn(cal) ? 1 : 2;
                                LOG.info("Adding exception {} (type={}) for calendar {} on date {}", ex.name, calendarDate.exception_type, cal.service_id, date.toString());

                                if (service.calendar_dates.containsKey(date))
                                    throw new IllegalArgumentException("Duplicate schedule exceptions on " + date.toString());

                                service.calendar_dates.put(date, calendarDate);
                                calendarDateCount += 1;
                            }
                        }
                        feed.services.put(cal.service_id, service);
                    }
                    if (calendarDateCount == 0) {
                        LOG.info("No calendar dates found. Skipping table.");
                    } else {
                        LOG.info("Writing {} calendar dates from schedule exceptions", calendarDateCount);
                        new CalendarDate.Writer(feed).writeTable(zipOutputStream);
                    }
                } else {
                    // No calendar records exist, export calendar_dates as is and hope for the best.
                    // This situation will occur in at least 2 scenarios:
                    // 1.  A GTFS has been loaded into the editor that had only the calendar_dates.txt file
                    //     and no further edits were made before exporting to a snapshot
                    // 2.  A new GTFS has been created from scratch and calendar information has yet to be added.
                    //     This will result in an invalid GTFS, but it was what the user wanted so ¯\_(ツ)_/¯
                    result.calendarDates = export(Table.CALENDAR_DATES, connection);
                }
            } else {
                // Otherwise, simply export the calendar dates as they were loaded in.
                result.calendarDates = export(Table.CALENDAR_DATES, connection);
            }
            result.fareAttributes = export(Table.FARE_ATTRIBUTES, connection);
            result.fareRules = export(Table.FARE_RULES, connection);
            result.feedInfo = export(Table.FEED_INFO, connection);
            // Only write frequencies for "approved" routes using COPY TO with results of select query
            if (fromEditor) {
                // Generate filter SQL for trips if exporting a feed/schema that represents an editor snapshot.
                // The filter clause for frequencies requires two joins to reach the routes table and a where filter on
                // route status.
                // FIXME Replace with string literal query instead of clause generators
                result.frequencies = export(
                    Table.FREQUENCIES,
                    String.join(
                        " ",
                        // Convert start_time and end_time values from seconds to time format (HH:MM:SS).
                        Table.FREQUENCIES.generateSelectSql(feedIdToExport, Requirement.OPTIONAL),
                        Table.FREQUENCIES.generateJoinSql(Table.TRIPS, feedIdToExport),
                        Table.TRIPS.generateJoinSql(
                            Table.ROUTES,
                            feedIdToExport,
                            "route_id",
                            false
                        ),
                        whereRouteIsApproved
                    )
                );
            } else {
                result.frequencies = export(Table.FREQUENCIES, connection);
            }

            // Only write "approved" routes using COPY TO with results of select query
            if (fromEditor) {
                // The filter clause for routes is simple. We're just checking that the route is APPROVED.
                result.routes = export(
                    Table.ROUTES,
                    String.join(
                        " ",
                        Table.ROUTES.generateSelectSql(feedIdToExport, Requirement.OPTIONAL),
                        whereRouteIsApproved
                    )
                );
            } else {
                result.routes = export(Table.ROUTES, connection);
            }

            // FIXME: Find some place to store errors encountered on export for patterns and pattern stops.
            // FIXME: Is there a need to export patterns or pattern stops? Should these be iterated over to ensure that
            // frequency-based pattern travel times match stop time arrivals/departures?
//            export(Table.PATTERNS);
//            export(Table.PATTERN_STOP);
            // Only write shapes for "approved" routes using COPY TO with results of select query
            if (fromEditor) {
                // Generate filter SQL for shapes if exporting a feed/schema that represents an editor snapshot.
                // The filter clause for shapes requires joining to trips and then to routes table and a where filter on
                // route status.
                // FIXME: I'm not sure that shape_id is indexed for the trips table. This could cause slow downs.
                // FIXME: this is exporting point_type, which is not a GTFS field, but its presence shouldn't hurt.
                String shapeFieldsToExport = Table.commaSeparatedNames(
                    Table.SHAPES.specFields(),
                    String.join(".", feedIdToExport, Table.SHAPES.name + "."),
                    true
                );
                // NOTE: The below substitution uses relative indexing. All values "%<s" reference the same arg as the
                // previous format specifier (i.e., feedIdToExport).
                result.shapes = export(
                    Table.SHAPES,
                    String.format(
                        "select %s " +
                            "from (select distinct %s.trips.shape_id " +
                            "from %<s.trips, %<s.routes " +
                            "where %<s.trips.route_id = %<s.routes.route_id and " +
                            "%<s.routes.status = 2) as unique_approved_shape_ids, " +
                            "%<s.shapes where unique_approved_shape_ids.shape_id = %<s.shapes.shape_id",
                        shapeFieldsToExport,
                        feedIdToExport
                    )
                );
            } else {
                result.shapes = export(Table.SHAPES, connection);
            }
            result.stops = export(Table.STOPS, connection);
            // Only write stop times for "approved" routes using COPY TO with results of select query
            if (fromEditor) {
                // Generate filter SQL for trips if exporting a feed/schema that represents an editor snapshot.
                // The filter clause for stop times requires two joins to reach the routes table and a where filter on
                // route status.
                // FIXME Replace with string literal query instead of clause generators
                result.stopTimes = export(
                    Table.STOP_TIMES,
                    String.join(" ",
                        Table.STOP_TIMES.generateSelectSql(feedIdToExport, Requirement.OPTIONAL),
                        Table.STOP_TIMES.generateJoinSql(Table.TRIPS, feedIdToExport),
                        Table.TRIPS.generateJoinSql(
                            Table.ROUTES,
                            feedIdToExport,
                            "route_id",
                            false
                        ),
                        whereRouteIsApproved
                    )
                );
            } else {
                result.stopTimes = export(Table.STOP_TIMES, connection);
            }
            result.transfers = export(Table.TRANSFERS, connection);
            if (fromEditor) {
                // Generate filter SQL for trips if exporting a feed/schema that represents an editor snapshot.
                // The filter clause for trips requires an inner join on the routes table and the same where check on
                // route status.
                // FIXME Replace with string literal query instead of clause generators
                result.trips = export(
                    Table.TRIPS,
                    String.join(" ",
                        Table.TRIPS.generateSelectSql(feedIdToExport, Requirement.OPTIONAL),
                        Table.TRIPS.generateJoinSql(
                            Table.ROUTES,
                            feedIdToExport,
                            "route_id",
                            false
                        ),
                        whereRouteIsApproved
                    )
                );
            } else {
                result.trips = export(Table.TRIPS, connection);
            }
            // Location meta data and location shapes are exported at the same time. The result will be the same for both.
            result.locationMetaData = exportLocations();
            result.locationShapes = result.locationMetaData;

            zipOutputStream.close();
            // Run clean up on the resulting zip file.
            cleanUpZipFile();
            result.completionTime = System.currentTimeMillis();
            result.loadTimeMillis = result.completionTime - startTime;
            // Exporting primary GTFS tables for GRTA Xpress = 12 sec
            LOG.info("Exporting tables took {} sec", (result.loadTimeMillis) / 1000);
            LOG.info("Exported feed {} to zip file: {}", feedIdToExport, outFile);
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
     * Removes any empty zip files from the final zip file.
     */
    private void cleanUpZipFile() {
        long startTime = System.currentTimeMillis();
        // Define ZIP File System Properties in HashMap
        Map<String, String> zip_properties = new HashMap<>();
        // We want to read an existing ZIP File, so we set this to False
        zip_properties.put("create", "false");

        // Specify the path to the ZIP File that you want to read as a File System
        // (File#toURI allows this to work across different operating systems, including Windows)
        URI zip_disk = URI.create("jar:" + new File(outFile).toURI());

        // Create ZIP file System
        try (FileSystem fileSystem = FileSystems.newFileSystem(zip_disk, zip_properties)) {
            // Get the Path inside ZIP File to delete the ZIP Entry
            for (String fileName : emptyTableList) {
                Path filePath = fileSystem.getPath(fileName);
                // Execute Delete
                Files.delete(filePath);
                LOG.info("Empty file {} successfully deleted", fileName);
            }
        } catch (IOException e) {
            LOG.error("Could not remove empty zip files");
            e.printStackTrace();
        }
        LOG.info("Deleted {} empty files in {} ms", emptyTableList.size(), System.currentTimeMillis() - startTime);
    }

    private TableLoadResult export (Table table, Connection connection) {
        if (fromEditor) {
            // Default behavior for exporting editor snapshot tables is to select only the spec fields.
            return export(table, table.generateSelectSql(feedIdToExport, Requirement.OPTIONAL));
        } else {
            String existingFieldsSelect = null;
            try {
                existingFieldsSelect = table.generateSelectAllExistingFieldsSql(connection, feedIdToExport);
            } catch (SQLException e) {
                LOG.error("failed to generate select statement for existing fields");
                TableLoadResult tableLoadResult = new TableLoadResult();
                tableLoadResult.fatalException = e.toString();
                e.printStackTrace();
                return tableLoadResult;
            }
            return export(table, existingFieldsSelect);
        }
    }

    /**
     * Export a table to the zipOutputStream to be written to the GTFS.
     */
    private TableLoadResult export (Table table, String filterSql) {
        long startTime = System.currentTimeMillis();
        TableLoadResult tableLoadResult = new TableLoadResult();
        try {
            if (filterSql == null) {
                throw new IllegalArgumentException("filterSql argument cannot be null");
            } else {
                // Surround filter SQL in parentheses.
                filterSql = String.format("(%s)", filterSql);
            }

            // Create entry for table
            String textFileName = table.name + ".txt";
            ZipEntry zipEntry = new ZipEntry(textFileName);
            zipOutputStream.putNextEntry(zipEntry);

            // don't let CSVWriter close the stream when it is garbage-collected
            OutputStream protectedOut = new FilterOutputStream(zipOutputStream);
            String copySql = String.format("copy %s to STDOUT DELIMITER ',' CSV HEADER", filterSql);
            LOG.info(copySql);
            // Our connection pool wraps the Connection objects, so we need to unwrap the Postgres connection interface.
            CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
            tableLoadResult.rowCount = (int) copyManager.copyOut(copySql, protectedOut);
            if (tableLoadResult.rowCount == 0) {
                // If no rows were exported, keep track of table name for later removal.
                emptyTableList.add(textFileName);
            }
            zipOutputStream.closeEntry();
            LOG.info("Copied {} {} in {} ms.", tableLoadResult.rowCount, table.name, System.currentTimeMillis() - startTime);
            connection.commit();
        } catch (SQLException | IOException | IllegalArgumentException e) {
            // Rollback connection so that fatal exception does not impact loading of other tables.
            try {
                connection.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            tableLoadResult.fatalException = e.toString();
            LOG.error("Exception while exporting tables", e);
        }
        return tableLoadResult;
    }

    /**
     * Export location metadata and location shapes to a single locations.geojson file.
     */
    private TableLoadResult exportLocations() {
        long startTime = System.currentTimeMillis();
        TableLoadResult tableLoadResult = new TableLoadResult();
        try {
            final TableReader<LocationMetaData> locationMetaDataIterator
                = new JDBCTableReader(Table.LOCATION_META_DATA, dataSource, feedIdToExport + ".", EntityPopulator.LOCATION_META_DATA);
            final TableReader<LocationShape> locationShapesIterator
                = new JDBCTableReader(Table.LOCATION_SHAPES, dataSource, feedIdToExport + ".", EntityPopulator.LOCATION_SHAPES);
            List<LocationMetaData> locationMetaData = Lists.newArrayList(locationMetaDataIterator);
            List<LocationShape> locationShapes = Lists.newArrayList(locationShapesIterator);
            if (locationMetaData.size() > 0) {
                // Only export if data is available.
                tableLoadResult.rowCount = locationMetaData.size() + locationShapes.size();
                writeLocationsToFile(zipOutputStream, locationMetaData, locationShapes);
                LOG.info("Copied {} {} in {} ms.", tableLoadResult.rowCount, Table.locationGeoJsonFileName, System.currentTimeMillis() - startTime);
            } else {
                LOG.warn("No locations exported to {} as the {} table is empty!", Table.locationGeoJsonFileName, Table.LOCATION_META_DATA.name);
            }
        } catch (IOException | GeoJsonException e) {
            tableLoadResult.fatalException = e.toString();
            LOG.error("Exception while exporting {}", Table.locationGeoJsonFileName, e);
        }
        return tableLoadResult;
    }

    /**
     * Pack the locations data and write to zip file.
     */
    public static void writeLocationsToFile(
        ZipOutputStream zipOutputStream,
        List<LocationMetaData> locationMetaData,
        List<LocationShape> locationShapes
    ) throws IOException, GeoJsonException {
        // Create entry for table.
        zipOutputStream.putNextEntry(new ZipEntry(Table.locationGeoJsonFileName));
        // Create and use PrintWriter, but don't close. This is done when the zip entry is closed.
        PrintWriter p = new PrintWriter(zipOutputStream);
        p.println(GeoJsonUtil.packLocations(locationMetaData, locationShapes));
        p.flush();
        zipOutputStream.closeEntry();
    }
}
