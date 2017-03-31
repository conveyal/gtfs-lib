package com.conveyal.gtfs.loader;

import com.csvreader.CsvReader;
import org.apache.commons.io.input.BOMInputStream;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.conveyal.gtfs.model.Entity.human;

/**
 * After experimenting with loading GTFS CSV tables into Java objects and then using ORM to put those objects into
 * a database, we'll now experiment with loading straight from CSV into a database, but checking and cleaning the data while
 * doing so. Two different ways: batched prepared inserts and loading from comma or tab separated files.
 *
 * This class should check that:
 * 1. all rows have the same number of fields as there are headers
 * 2. all required fields are present
 * 3. all fields can be converted to the proper data types and are in range
 * 4. referential integrity?
 */
public class CsvLoader {

    private static final Logger LOG = LoggerFactory.getLogger(CsvLoader.class);
    private static final boolean COPY_OVER_NETWORK = true;

    protected Connection connection;
    protected Statement statement;
    protected CsvReader csvReader;
    protected long line;
    protected ZipFile zip;

    private File tempTextFile;
    private PrintStream tempTextFileStream;
    private PreparedStatement insertStatement = null;

    public CsvLoader(ZipFile zip) {
        this.zip = zip;
    }

    private final long INSERT_BATCH_SIZE = 500;

    /**
     * @param viaText will only work if the database server process has access to the local filesystem
     *                It uses the Postgres text format, so it's somewhat tied to that RDBMS.
     */
    public void load (Table table, boolean viaText) throws Exception {
        ZipEntry entry = zip.getEntry(table.name + ".txt");
        if (entry == null) {
            // check for TableInSubdirectoryError and see if table is required
        }
        LOG.info("Loading GTFS table {} from {}", table.name, entry);
        InputStream zipInputStream = zip.getInputStream(entry);
        // Skip any byte order mark that may be present. Files must be UTF-8,
        // but the GTFS spec says that "files that include the UTF byte order mark are acceptable".
        InputStream bomInputStream = new BOMInputStream(zipInputStream);
        csvReader = new CsvReader(bomInputStream, ',', Charset.forName("UTF8"));
        csvReader.readHeaders();

        // TODO Strip out line returns, tabs.
        // By default the CSV reader trims leading and trailing whitespace in fields.

        // Build up a list of fields in the same order they appear in this GTFS CSV file.
        Field[] fields = new Field[csvReader.getHeaderCount()];
        Set<String> fieldsSeen = new HashSet<>();
        for (int h = 0; h < csvReader.getHeaderCount(); h++) {
            String header = csvReader.getHeader(h);
            if (fieldsSeen.contains(header)) {
                // Error: duplicate field name
                fields[h] = null;
            } else {
                fields[h] = table.getFieldForHeader(header);
                fieldsSeen.add(header);
            }
        }

        // Replace the GTFS spec table with the actual SQL table we are populating.
        table = new Table(table.name, fields);

        // final String URL = "jdbc:h2:file:~/test-db";
        // final String URL = "jdbc:h2:mem:";
        final String URL = "jdbc:postgresql://localhost/catalogue";
        // Driver should auto-register these days.
        connection = DriverManager.getConnection(URL);
        // Statement statement = connection.createStatement();
        // statement.execute("create schema if not exists nl");
        // connection.setSchema()
        connection.setAutoCommit(false);
        statement = connection.createStatement();

        // Some databases require the table to exist before a statement can be prepared.
        table.createSqlTable(connection);

        if (viaText) {
            // No need to output headers to temp text file, our SQL table column order exactly matches our text file.
            tempTextFile = File.createTempFile(table.name, "text");
            tempTextFileStream = new PrintStream(new BufferedOutputStream(new FileOutputStream(tempTextFile)));
            LOG.info("Loading via temporary text file at " + tempTextFile.getAbsolutePath());
        } else {
            insertStatement = connection.prepareStatement(table.generateInsertSql());
            LOG.info(insertStatement.toString()); // Logs the SQL for the prepared statement
        }

        // When outputting text, accumulate transformed strings to allow skipping rows when errors are encountered.
        String[] transformedStrings = new String[fields.length];

        while (csvReader.readRecord()) {
            // The CSV reader's current record is zero-based and does not include the header line.
            // Convert to a CSV file line number that will make more sense to people reading error messages.
            long line = csvReader.getCurrentRecord() + 2;
            if (line % 500_000 == 0) LOG.info("Processed {}", human(line));
            if (csvReader.getColumnCount() != fields.length) {
                LOG.error("Wrong number of fields in CSV row. Skipping it.");
                continue;
            }
            for (int f = 0; f < fields.length; f++) {
                Field field = fields[f];
                String string = csvReader.get(f);
                if (string == null || string.isEmpty()) string = "0"; // FIXME Need to define default values
                if (viaText) transformedStrings[f] = field.validateAndConvert(string);
                else field.setPreparedStatementParameter(f + 1, string, insertStatement);
            }
            if (viaText) {
                tempTextFileStream.printf(String.join("\t", transformedStrings));
                tempTextFileStream.print('\n');
            } else {
                insertStatement.addBatch();
                if (line % INSERT_BATCH_SIZE == 0) insertStatement.executeBatch();
            }
        }

        if (viaText) {
            LOG.info("Loading into database table from temporary text file...");
            if (COPY_OVER_NETWORK) {
                // Allows sending over network, only slightly slower
                final String copySql = String.format("copy %s from stdin", table.name);
                InputStream stream = new BufferedInputStream(new FileInputStream(tempTextFile.getAbsolutePath()));
                CopyManager copyManager = new CopyManager((BaseConnection) connection);
                copyManager.copyIn(copySql, stream, 1024*1024);
                stream.close();
            } else {
                // Load from local file on the database server
                statement.execute(String.format("copy %s from '%s'", table.name, tempTextFile.getAbsolutePath()));
            }
        } else {
            insertStatement.executeBatch();
        }

        LOG.info("Indexing...");
        // statement.execute("create index on stop_times (trip_id, stop_sequence)");

        LOG.info("Committing transaction...");
        connection.commit();
        LOG.info("Done.");
    }

    public static void main (String[] args) {

        //final String file = "/Users/abyrd/r5/pdx/portland-2016-08-22.gtfs.zip";
        final String file = "/Users/abyrd/geodata/nl/NL-OPENOV-20170322-gtfs.zip";
        try {
            final ZipFile zip = new ZipFile(file);
            final CsvLoader loader = new CsvLoader(zip);
            loader.load(Table.routes, false);
            loader.load(Table.stops, false);
            loader.load(Table.trips, true);
            loader.load(Table.stop_times, true);
            loader.load(Table.shapes, true);
            zip.close();
        } catch (Exception ex) {
            LOG.error("Error loading GTFS: {}", ex.getMessage());
            throw new RuntimeException(ex);
        }

    }

}
