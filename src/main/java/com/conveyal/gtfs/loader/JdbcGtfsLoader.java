package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.storage.StorageException;
import com.csvreader.CsvReader;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.math3.random.MersenneTwister;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
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
public class JdbcGtfsLoader {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcGtfsLoader.class);
    private static final long INSERT_BATCH_SIZE = 500;

    private String gtfsFilePath;
    protected ZipFile zip;

    private File tempTextFile;
    private PrintStream tempTextFileStream;
    private PreparedStatement insertStatement = null;

    private final DataSource dataSource;
    private SQLErrorStorage errorStorage;
    private String tablePrefix;

    public JdbcGtfsLoader(String gtfsFilePath, DataSource dataSource) {
        this.gtfsFilePath = gtfsFilePath;
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
    public String loadTables () {
        try {
            File gtfsFile = new File(gtfsFilePath);

            long startTime = System.currentTimeMillis();
            HashCode md5 = Files.hash(gtfsFile, Hashing.md5());
            String md5Hex = md5.toString();
            LOG.info("MD5 took {} msec, {}", System.currentTimeMillis() - startTime, md5Hex);

            startTime = System.currentTimeMillis();
            HashCode sha1 = Files.hash(gtfsFile, Hashing.sha1());
            String shaHex = sha1.toString();
            LOG.info("SHA1 took {} msec, {}", System.currentTimeMillis() - startTime, shaHex);

            this.zip = new ZipFile(gtfsFilePath);
            this.tablePrefix = makeTablePrefix(); // TODO handle case where we don't want any prefix. Method must still run to create feed_info table.
            this.errorStorage = new SQLErrorStorage(dataSource, tablePrefix, true);

            startTime = System.currentTimeMillis();
            // FIXME: load remaining tables
            load(Table.AGENCIES);
            load(Table.ROUTES);
            load(Table.STOPS);
            load(Table.TRIPS);
            load(Table.SHAPES);
            load(Table.STOP_TIMES);
            errorStorage.commitAndClose();
            zip.close();
            LOG.info("Loading tables took {} sec", (System.currentTimeMillis() - startTime) / 1000);
        } catch (Exception ex) {
            LOG.error("Exception while loading GTFS file: {}", ex.toString());
            ex.printStackTrace();
            return null;
        }
        return tablePrefix;
    }


    /**
     * Really this is not just making the table prefix - it's loading the feed_info and should also calculate hashes.
     */
    private String makeTablePrefix () {
        // First inspect feed_info.txt to extract some information.
        CsvReader csvReader = getCsvReader("feed_info.txt");
        String feedId = "", feedVersion = "";
        if (csvReader != null) {
            // feed_info.txt has been found and opened.
            try {
                csvReader.readRecord();
                // csvReader.get() returns the empty string for missing columns
                feedId = csvReader.get("feed_id");
                feedVersion = csvReader.get("feed_version");
            } catch (IOException e) {
                LOG.error("Exception while inspecting feed_info: {}", e);
            }
            csvReader.close();
        }
        // Now create a row for this feed
        String tablePrefix = randomIdString();
        // feed_id and feed_version based schema names get messy. We'll just use random unique IDs for now.
        // FIXME do this in a loop just in case there's an ID collision.
        // We don't want to use an auto-increment primary key because as table names these need to be alphabetical.
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
            // FIXME do this only on databases that support schemas.
            // SQLite does not support them. Is there any advantage of schemas over flat tables?
            statement.execute("create schema " + tablePrefix);
            // TODO load more stuff from feed_info and essentially flatten all feed_infos from all loaded feeds into one table
            // This should include date range etc. Can we reuse any code from Table for this?
            // This makes sense since the file should only have one line.
            // current_timestamp seems to be the only standard way to get the current time across all common databases.
            statement.execute("create table if not exists feed_info (namespace varchar primary key, " +
                    "feed_id varchar, feed_version varchar, filename varchar, loaded_date timestamp)");
            PreparedStatement insertStatement = connection.prepareStatement(
                    "insert into feed_info values (?, ?, ?, ?, current_timestamp)");
            insertStatement.setString(1, tablePrefix);
            insertStatement.setString(2, feedId); // TODO set null when missing
            insertStatement.setString(3, feedVersion); // TODO set null when missing
            insertStatement.setString(4, zip.getName());
            insertStatement.execute();
            // TODO add feed checksum to this table
            connection.commit();
            // Close all statements, results, and prepared statements, returning the connection to the pool.
            connection.close();
            LOG.info("Created new feed namespace: {}", insertStatement);
            tablePrefix += ".";
        } catch (SQLException e) {
            LOG.error("Exception while creating unique prefix for new feed: {}", e.getMessage());
        } finally {
            DbUtils.closeQuietly(connection);
        }
        return tablePrefix;
    }

    /**
     * Generate a random unique prefix of n lowercase letters.
     * We can't count on sql table or schema names being case sensitive or tolerating (leading) digits.
     * For n=10, number of possibilities is 26^10 or 1.4E14.
     *
     * The approximate probability of a hash collision is k^2/2H where H is the number of possible hash values and
     * k is the number of items hashed.
     *
     * SHA1 is 160 bits, MD5 is 128 bits, and UUIDs are 128 bits with only 122 actually random.
     * To reach the uniqueness of a UUID you need math.log(2**122, 26) or about 26 letters.
     * An MD5 can be represented as 32 hex digits so we don't save much length, but we do make it entirely alphabetical.
     * log base 2 of 26 is about 4.7, so each character represents about 4.7 bits of randomness.
     *
     * The question remains of whether we need globally unique IDs or just application-unique IDs. The downside of
     * generating IDs sequentially or with little randomness is that when multiple databases are created we'll have
     * feeds with the same IDs as older or other databases, allowing possible accidental collisions with strange
     * failure modes.
     */
    private String randomIdString() {
        MersenneTwister twister = new MersenneTwister();
        final int length = 27;
        char[] chars = new char[length];
        for (int i = 0; i < length; i++) {
            chars[i] = (char) ('a' + twister.nextInt(26));
        }
        // Add a visual separator, which makes these easier to distinguish at a glance
        chars[4] = '_';
        return new String(chars);
    }

    /**
     * In GTFS feeds, all files are supposed to be in the root of the zip file, but feed producers often put them
     * in a subdirectory. This function will search subdirectories if the entry is not found in the root.
     * It records an error if the entry is in a subdirectory.
     * It then creates a CSV reader for that table if it's found.
     */
    private CsvReader getCsvReader (String tableFileName) {
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
        if (entry == null) return null;
        try {
            InputStream zipInputStream = zip.getInputStream(entry);
            // Skip any byte order mark that may be present. Files must be UTF-8,
            // but the GTFS spec says that "files that include the UTF byte order mark are acceptable".
            InputStream bomInputStream = new BOMInputStream(zipInputStream);
            CsvReader csvReader = new CsvReader(bomInputStream, ',', Charset.forName("UTF8"));
            csvReader.readHeaders();
            return csvReader;
        } catch (IOException e) {
            LOG.error("Exception while opening zip entry: {}", e);
            e.printStackTrace();
            return null;
        }
    }

    private void load (Table table) throws Exception {
        final String tableFileName = table.name + ".txt";
        CsvReader csvReader = getCsvReader(tableFileName);
        if (csvReader == null) {
            // This GTFS table could not be opened in the zip, even in a subdirectory.
            if (table.isRequired()) errorStorage.storeError(new NewGTFSError(MISSING_TABLE, tableFileName));
            return;
        }
        LOG.info("Loading GTFS table {}", table.name);
        Connection connection = dataSource.getConnection();
        // Use the Postgres text load format if we're connected to that DBMS.
        boolean postgresText = (connection.getMetaData().getDatabaseProductName().equals("PostgreSQL"));

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
        Table targetTable = new Table(tablePrefix + table.name, table.entityClass, table.isRequired(), fields);

        // NOTE H2 doesn't seem to work with schemas (or create schema doesn't work).
        // With bulk loads it takes 140 seconds to load the data and addditional 120 seconds just to index the stop times.
        // SQLite also doesn't support schemas, but you can attach additional database files with schema-like naming.
        // We'll just literally prepend feed indentifiers to table names when supplied.

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
            final String copySql = String.format("copy %s from stdin", targetTable.name);
            InputStream stream = new BufferedInputStream(new FileInputStream(tempTextFile.getAbsolutePath()));
            // Our connection pool wraps the Connection objects, so we need to unwrap the Postgres connection interface.
            CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
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
        String indexName = String.join("_", targetTable.name.replace(".", "_"), "idx");
        String indexSql = String.format("create index %s on %s (%s)", indexName, targetTable.name, indexColumns);
        //String indexSql = String.format("alter table %s add primary key (%s)", table.name, indexColumns);
        LOG.info(indexSql);
        connection.createStatement().execute(indexSql);
        // TODO add foreign key constraints, and recover recording errors as needed.

        LOG.info("Committing transaction...");
        connection.commit();
        connection.close();
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
        String clean = string.replaceAll("[^\\p{Alnum}_]", "");
        if (!clean.equals(string)) {
            LOG.warn("SQL identifier '{}' was sanitized to '{}'", string, clean);
            if (errorStorage != null) {
                errorStorage.storeError(new NewGTFSError(TABLE_NAME_FORMAT, string));
            }
        }
        return clean;
    }

}
