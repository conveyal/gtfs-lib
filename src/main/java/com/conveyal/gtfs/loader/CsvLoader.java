package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.storage.StorageException;
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
 * This class loads CSV tables from zipped GTFS into an SQL relational database management system.
 * Comparing the GTFS specification for a table with the headers present in the GTFS CSV, it dynamically builds up a
 * table definitions and SQL statements to interact with those tables. It retains all columns present in the GTFS,
 * including optional columns, known extensions, and unrecognized proprietary extensions.
 *
 * batched prepared inserts and loading from comma or tab separated files.
 *
 * Our previous approach involved loading GTFS CSV tables into Java objects and then using an object-relational mapping
 * to put those objects into a database. In that case a fixed number of fields are represented. If the GTFS feed
 * contains extra proprietary fields, they are lost immediately on import. The Java model objects must contain fields
 * for all GTFS columns that will ever be retrieved, and memory and database space are required to represent all those
 * fields even when they are not present in a particular feed. The same would be true of a direct-to-database approach
 * if multiple feeds were loaded into the same tables: a "lowest common denominator" set of fields would need to be
 * selected. However, we create one set of tables per GTFS feed loaded into the same database and namespace them with
 * what the SQL spec calls "schemas".
 *
 * The structure of the CSV file and the data it contains are validated line by line during the load process. This
 * allows us to handle error recovery ourselves, recording detailed messages about the largest possible number of errors
 * rather than failing at the first error.
 *
 * This is important because of the generally long turnaround for GTFS publication, repair, and validation. If a newly
 * submitted feed fails to import because of a missing file, we don't want to report that single error to the feed
 * producer, only to discover and report additional errors when the repaired feed is re-submitted weeks later.
 *
 * The fact that existing libraries would abort a GTFS import upon encountering very common, recoverable errors was the
 * original motivation for creating gtfs-lib. Error recovery is all the more important when bulk-loading data into
 * database systems - the Postgres 'copy' import is particularly brittle and does not provide error messages that would
 * help the feed producer repair their feed.
 *
 * The validation performed during CSV loading includes:
 * - columns are present for all required fields
 * - all rows have the same number of fields as there are headers
 * - fields do not contain problematic characters
 * - field contents can be converted to the target data types and are in range
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
            String header = sanitize(csvReader.getHeader(h));
            if (fieldsSeen.contains(header)) {
                // Error: duplicate field name TODO deal with missing (null) Field object below
                fields[h] = null;
            } else {
                fields[h] = table.getFieldForName(header);
                fieldsSeen.add(header);
            }
        }

        // Replace the GTFS spec Table with one representing the SQL table we will populate.
        table = new Table(table.name, fields);

        final String h2_file_url = "jdbc:h2:file:~/test-db";
        final String h2_mem_url = "jdbc:h2:mem:";
        final String postgres_local_url = "jdbc:postgresql://localhost/catalogue";
        // JODBC drivers should auto-register these days. You used to have to trick the class loader into loading them.
        connection = DriverManager.getConnection(postgres_local_url);
        connection.setAutoCommit(false);
        // TODO set up schemas
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
            long lineNumber = csvReader.getCurrentRecord() + 2;
            if (lineNumber % 500_000 == 0) LOG.info("Processed {}", human(lineNumber));
            if (csvReader.getColumnCount() != fields.length) {
                LOG.error("Wrong number of fields in CSV row. Skipping it.");
                continue;
            }
            for (int f = 0; f < fields.length; f++) {
                Field field = fields[f];
                String string = csvReader.get(f);
                if (string.isEmpty()) { // TODO verify that CSV reader always returns empty strings, not nulls
                    if (field.isRequired()) throw new StorageException("Missing required field."); // TODO record and recover.
                    if (viaText) transformedStrings[f] = "\\N"; // Represents null in Postgres text format
                    else insertStatement.setNull(f + 1, field.getSqlType().getVendorTypeNumber());
                } else {
                    if (viaText) transformedStrings[f] = field.validateAndConvert(string);
                    // Micro-benchmarks show it's 4-5% faster to call the type-specific parameter setters rather than setObject with a type code.
                    else field.setParameter(insertStatement, f + 1, string);
                }
            }
            if (viaText) {
                tempTextFileStream.printf(String.join("\t", transformedStrings));
                tempTextFileStream.print('\n');
            } else {
                insertStatement.addBatch();
                if (lineNumber % INSERT_BATCH_SIZE == 0) insertStatement.executeBatch();
            }
        }

        if (viaText) {
            LOG.info("Loading into database table from temporary text file...");
            tempTextFileStream.close();
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
        // TODO build all indexes by just running an SQL script.

        LOG.info("Committing transaction...");
        connection.commit();
        LOG.info("Done.");
    }

    /**
     * Protect against SQL injection.
     * The only place we include arbitrary input in SQL is the column names of tables.
     * Implicitly (looking at all existing table names) these should consist entirely of
     * lowercase letters and underscores.
     *
     * TODO test including SQL injection text (quote and semicolon)
     */
    private static String sanitize (String string) {
        String clean = string.replace("\\W", "");
        if (!clean.equals(string)) {
            // TODO recover and record error.
            throw new StorageException("Column header includes illegal characters: " + string);
        }
        return clean;
    }

    public static void main (String[] args) {

        final String pdx_file = "/Users/abyrd/r5/pdx/portland-2016-08-22.gtfs.zip";
        final String nl_file = "/Users/abyrd/geodata/nl/NL-OPENOV-20170322-gtfs.zip";
        try {
            final ZipFile zip = new ZipFile(nl_file);
            final CsvLoader loader = new CsvLoader(zip);
            loader.load(Table.routes, false);
            loader.load(Table.stops, false);
            loader.load(Table.trips, true);
//            loader.load(Table.stop_times, true);
//            loader.load(Table.shapes, true);
            zip.close();
        } catch (Exception ex) {
            LOG.error("Error loading GTFS: {}", ex.getMessage());
            throw new RuntimeException(ex);
        }

    }

}
