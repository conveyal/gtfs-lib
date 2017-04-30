package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.SQLErrorStorage;
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

import static com.conveyal.gtfs.error.NewGTFSErrorType.*;
import static com.conveyal.gtfs.model.Entity.human;

/**
 * This class loads CSV tables from zipped GTFS into an SQL relational database management system with a JDBC driver.
 * By comparing the GTFS specification for a table with the headers present in the GTFS CSV, it dynamically builds up
 * table definitions and SQL statements to interact with those tables. It retains all columns present in the GTFS,
 * including optional columns, known extensions, and unrecognized proprietary extensions.
 *
 * It supports several ways of putting the data into the tables: batched prepared inserts or loading from an
 * intermediate tab separated text file.
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
 * - TODO referential integrity
 */
public class CsvLoader {

    private static final Logger LOG = LoggerFactory.getLogger(CsvLoader.class);
    private static final long INSERT_BATCH_SIZE = 500;

    protected Connection connection;
    protected CsvReader csvReader;
    protected ZipFile zip;

    private File tempTextFile;
    private PrintStream tempTextFileStream;
    private PreparedStatement insertStatement = null;
    private ConnectionSource connectionSource = new ConnectionSource(ConnectionSource.POSTGRES_LOCAL_URL);

    private SQLErrorStorage errorStorage;

    public CsvLoader (ZipFile zip) {
        this.zip = zip;
        this.connection = connectionSource.getConnection(null);
        this.errorStorage = new SQLErrorStorage(connection, true);
    }

    public void load (Table table) throws Exception {
        final String tableFileName = table.name + ".txt";
        ZipEntry entry = zip.getEntry(tableFileName);
        if (entry == null) {
            // Table was not found, check if it is in a subdirectory.
            Enumeration<? extends ZipEntry> entries = zip.entries();
            while (entries.hasMoreElements()) {
                ZipEntry e = entries.nextElement();
                if (e.getName().endsWith(tableFileName)) {
                    entry = e;
                    errorStorage.storeError(new NewGTFSError(TABLE_IN_SUBDIRECTORY, e.getName()));
                    break;
                }
            }
        }
        if (entry == null) {
            // This GTFS table did not exist in the zip, even in a subdirectory.
            if (table.isRequired()) errorStorage.storeError(new NewGTFSError(MISSING_TABLE, tableFileName));
            return;
        }

        // Use the Postgres text load format if we're connected to that DBMS.
        boolean postgresText = (connection instanceof BaseConnection);

        LOG.info("Loading GTFS table {} from {}", table.name, entry);
        InputStream zipInputStream = zip.getInputStream(entry);
        // Skip any byte order mark that may be present. Files must be UTF-8,
        // but the GTFS spec says that "files that include the UTF byte order mark are acceptable".
        InputStream bomInputStream = new BOMInputStream(zipInputStream);
        csvReader = new CsvReader(bomInputStream, ',', Charset.forName("UTF8"));
        csvReader.readHeaders();

        // TODO Strip out line returns, tabs in field contents.
        // By default the CSV reader trims leading and trailing whitespace in fields.

        // Build up a list of fields in the same order they appear in this GTFS CSV file.
        Field[] fields = new Field[csvReader.getHeaderCount()];
        Set<String> fieldsSeen = new HashSet<>();
        for (int h = 0; h < csvReader.getHeaderCount(); h++) {
            String header = sanitize(csvReader.getHeader(h));
            if (fieldsSeen.contains(header)) {
                String badValues = String.format("file=%s; header=%s", tableFileName, header);
                errorStorage.storeError(new NewGTFSError(DUPLICATE_HEADER, badValues));
                // TODO deal with missing (null) Field object below
                fields[h] = null;
            } else {
                fields[h] = table.getFieldForName(header);
                fieldsSeen.add(header);
            }
        }

        // Replace the GTFS spec Table with one representing the SQL table we will populate, with reordered columns.
        // FIXME this is confusing, we only create a new table object so we can call a couple of methods on it, all of which just need a list of fields.
        Table targetTable = new Table(table.name, table.entityClass, table.isRequired(), fields);

        // NOTE H2 doesn't seem to work with schemas (or create schema doesn't work).
        // With bulk loads it takes 140 seconds to load the data and addditional 120 seconds just to index the stop times.
        // SQLite also doesn't support schemas, but you can attach additional database files with schema-like naming.
        connection = connectionSource.getConnection(null);//"nl");

        // Some databases require the table to exist before a statement can be prepared.
        targetTable.createSqlTable(connection);

        // TODO are we loading with or without a header row in our Postgres text file?
        if (postgresText) {
            // No need to output headers to temp text file, our SQL table column order exactly matches our text file.
            tempTextFile = File.createTempFile(targetTable.name, "text");
            tempTextFileStream = new PrintStream(new BufferedOutputStream(new FileOutputStream(tempTextFile)));
            LOG.info("Loading via temporary text file at " + tempTextFile.getAbsolutePath());
        } else {
            insertStatement = connection.prepareStatement(targetTable.generateInsertSql());
            LOG.info(insertStatement.toString()); // Logs the SQL for the prepared statement
        }

        // When outputting text, accumulate transformed strings to allow skipping rows when errors are encountered.
        // One extra position in the array for the CSV line number.
        String[] transformedStrings = new String[fields.length + 1];

        while (csvReader.readRecord()) {
            // The CSV reader's current record is zero-based and does not include the header line.
            // Convert to a CSV file line number that will make more sense to people reading error messages.
            if (csvReader.getCurrentRecord() + 2 > Integer.MAX_VALUE) {
                errorStorage.storeError(new NewGTFSError(TABLE_TOO_LONG, table.name));
                break;
            }
            int lineNumber = ((int) csvReader.getCurrentRecord()) + 2;
            if (lineNumber % 500_000 == 0) LOG.info("Processed {}", human(lineNumber));
            if (csvReader.getColumnCount() != fields.length) {
                String badValues = String.format("expected=%d; found=%d", fields.length, csvReader.getColumnCount());
                errorStorage.storeError(new NewGTFSError(WRONG_NUMBER_OF_FIELDS, badValues, table.getEntityClass(), lineNumber));
                continue;
            }
            // The first field holds the line number of the CSV file. Prepared statement parameters are one-based.
            if (postgresText) transformedStrings[0] = Integer.toString(lineNumber);
            else insertStatement.setInt(1, lineNumber);
            for (int f = 0; f < fields.length; f++) {
                Field field = fields[f];
                String string = csvReader.get(f);
                if (string.isEmpty()) {
                    // TODO verify that CSV reader always returns empty strings, not nulls
                    if (field.isRequired()) {
                        errorStorage.storeError(new NewGTFSError(MISSING_FIELD, field.name, table.getEntityClass(), lineNumber));
                    }
                    if (postgresText) transformedStrings[f + 1] = "\\N"; // Represents null in Postgres text format
                    // Adjust parameter index by two: indexes are one-based and the first one is the CSV line number.
                    else insertStatement.setNull(f + 2, field.getSqlType().getVendorTypeNumber());
                } else {
                    // Micro-benchmarks show it's only 4-5% faster to call typed parameter setter methods
                    // rather than setObject with a type code. I think some databases don't have setObject though.
                    // The Field objects throw exceptions to avoid passing the line number, table name etc. into them.
                    try {
                        // FIXME we need to set the transformed string element even when an error occurs.
                        // This means the validation and insertion step need to happen separately.
                        if (postgresText) transformedStrings[f + 1] = field.validateAndConvert(string);
                        else field.setParameter(insertStatement, f + 2, string);
                    } catch (StorageException ex) {
                        String badValues = String.format("table=%s, line=%d", table.name, lineNumber);
                        errorStorage.storeError(new NewGTFSError(ex.errorType, badValues));
                    }
                }
            }
            if (postgresText) {
                tempTextFileStream.printf(String.join("\t", transformedStrings));
                tempTextFileStream.print('\n');
            } else {
                insertStatement.addBatch();
                if (lineNumber % INSERT_BATCH_SIZE == 0) insertStatement.executeBatch();
            }
        }

        // Finalize loading the table, either by copying the pre-validated text file into the database (for Postgres)
        // or inserting any remaining rows (for all others).

        if (postgresText) {
            LOG.info("Loading into database table from temporary text file...");
            tempTextFileStream.close();
            // Allows sending over network. This is only slightly slower than a local file copy.
            final String copySql = String.format("copy %s from stdin", table.name);
            InputStream stream = new BufferedInputStream(new FileInputStream(tempTextFile.getAbsolutePath()));
            CopyManager copyManager = new CopyManager((BaseConnection) connection);
            copyManager.copyIn(copySql, stream, 1024*1024);
            stream.close();
            // It is also possible to load from local file if this code is running on the database server.
            // statement.execute(String.format("copy %s from '%s'", table.name, tempTextFile.getAbsolutePath()));
        } else {
            insertStatement.executeBatch();
        }

        LOG.info("Indexing...");
        // We determine which columns should be indexed based on field order in the GTFS spec model table.
        // Not sure that's a good idea, this could use some abstraction. TODO getIndexColumns() on each table.
        String indexColumns = table.getIndexFields();
        // TODO verify referential integrity and uniqueness of keys
        // TODO create primary key and fall back on plain index (consider not null & unique constraints)
        // TODO use line number as primary key
        // Note: SQLITE requires specifying a name for indexes.
        String indexName = String.join("_", table.name, "idx");
        String indexSql = String.format("create index %s on %s (%s)", indexName, table.name, indexColumns);
        //String indexSql = String.format("alter table %s add primary key (%s)", table.name, indexColumns);
        LOG.info(indexSql);
        connection.createStatement().execute(indexSql);
        // TODO add foreign key constraints, and recover recording errors as needed.

        LOG.info("Committing transaction...");
        connection.commit();
        errorStorage.finish();
        LOG.info("Done.");
    }

    /**
     * Protect against SQL injection.
     * The only place we include arbitrary input in SQL is the column names of tables.
     * Implicitly (looking at all existing table names) these should consist entirely of
     * lowercase letters and underscores.
     *
     * TODO add a test including SQL injection text (quote and semicolon)
     */
    public String sanitize (String string) throws SQLException {
        String clean = string.replaceAll("[^\\p{Alnum}_-]", "");
        if (!clean.equals(string)) errorStorage.storeError(new NewGTFSError(TABLE_NAME_FORMAT, string));
        return clean;
    }

    public static void main (String[] args) {

        final String pdx_file = "/Users/abyrd/r5/pdx/portland-2016-08-22.gtfs.zip";
        final String nl_file = "/Users/abyrd/geodata/nl/NL-OPENOV-20170322-gtfs.zip";
        final String etang_file = "/Users/abyrd/r5/mamp-old/ETANG.GTFS.zip";
        try {
            // There appears to be no advantage to loading these in parallel, as this whole process is I/O bound.
            final ZipFile zip = new ZipFile(etang_file);
            final CsvLoader loader = new CsvLoader(zip);
            loader.load(Table.ROUTES);
            loader.load(Table.STOPS);
            loader.load(Table.TRIPS);
            loader.load(Table.SHAPES);
            loader.load(Table.STOP_TIMES);
            zip.close();
        } catch (Exception ex) {
            LOG.error("Error loading GTFS: {}", ex.getMessage());
            throw new RuntimeException(ex);
        }

    }

}
