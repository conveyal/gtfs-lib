package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.model.Entity;
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
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Handles exporting a feed contained in the database to a GTFS zip file.
 */
public class JdbcGtfsExporter {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcGtfsExporter.class);

    private final String outFile;
    private final DataSource dataSource;

    // These fields will be filled in once feed snapshot begins.
    private Connection connection;
    private String tablePrefix;
    private ZipOutputStream zipOutputStream;
    // The reference feed ID (namespace) to copy.
    private final String feedIdToExport;

    public JdbcGtfsExporter(String feedId, String outFile, DataSource dataSource) {
        this.feedIdToExport = feedId;
        this.outFile = outFile;
        this.dataSource = dataSource;
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
            this.tablePrefix += ".";
            // Export each table in turn (by placing entry in zip output stream).
            // FIXME: NO non-fatal exception errors are being captured during copy operations.
            result.agency = export(Table.AGENCY);
            result.calendar = export(Table.CALENDAR);
            result.calendarDates = export(Table.CALENDAR_DATES);
            result.fareAttributes = export(Table.FARE_ATTRIBUTES);
            result.fareRules = export(Table.FARE_RULES);
            result.feedInfo = export(Table.FEED_INFO);
            result.frequencies = export(Table.FREQUENCIES);
            result.routes = export(Table.ROUTES);
            // FIXME: Find some place to store errors encountered on export for patterns and pattern stops.
            export(Table.PATTERNS);
            export(Table.PATTERN_STOP);
            result.shapes = export(Table.SHAPES);
            result.stops = export(Table.STOPS);
            result.stopTimes = export(Table.STOP_TIMES);
            result.transfers = export(Table.TRANSFERS);
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
            boolean postgresText = (connection.getMetaData().getDatabaseProductName().equals("PostgreSQL"));

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
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return tableLoadResult;
    }
}
