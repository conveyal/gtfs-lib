package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.model.Agency;
import com.conveyal.gtfs.model.Calendar;
import com.conveyal.gtfs.model.CalendarDate;
import com.conveyal.gtfs.model.Entity;
import com.conveyal.gtfs.model.FareAttribute;
import com.conveyal.gtfs.model.FareRule;
import com.conveyal.gtfs.model.FeedInfo;
import com.conveyal.gtfs.model.Frequency;
import com.conveyal.gtfs.storage.StorageException;
import com.csvreader.CsvReader;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.mapdb.Fun;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.*;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.conveyal.gtfs.error.NewGTFSErrorType.*;
import static com.conveyal.gtfs.model.Entity.human;
import static com.conveyal.gtfs.util.Util.randomIdString;

/**
 * This class loads a MapDB-based GTFSFeed into an RDBMS.
 */
public class JdbcGtfsConverter {

    public static final long INSERT_BATCH_SIZE = 500;
    // Represents null in Postgres text format
    private static final String POSTGRES_NULL_TEXT = "\\N";
    private static final Logger LOG = LoggerFactory.getLogger(JdbcGtfsConverter.class);

    private File tempTextFile;
    private PrintStream tempTextFileStream;
    private PreparedStatement insertStatement = null;

    private final GTFSFeed gtfsFeed;
    private final DataSource dataSource;

    // These fields will be filled in once feed loading begins.
    private Connection connection;
    private String tablePrefix;
    private SQLErrorStorage errorStorage;

    // Contains references to unique entity IDs during load stage used for referential integrity check.
    private JdbcGtfsLoader.ReferenceTracker referenceTracker;
    private ObjectMapper mapper;
    private String namespace;

    public JdbcGtfsConverter(GTFSFeed gtfsFeed, DataSource dataSource) {
        this.gtfsFeed = gtfsFeed;
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
    public FeedLoadResult loadTables () {

        // This result object will be returned to the caller to summarize the feed and report any critical errors.
        FeedLoadResult result = new FeedLoadResult();

        try {
            // Begin tracking time. FIXME: should this follow the connect/register and begin with the table loads?
            long startTime = System.currentTimeMillis();
            // We get a single connection object and share it across several different methods.
            // This ensures that actions taken in one method are visible to all subsequent SQL statements.
            // If we create a schema or table on one connection, then access it in a separate connection, we have no
            // guarantee that it exists when the accessing statement is executed.
            connection = dataSource.getConnection();
            // Generate a unique prefix that will identify this feed.
            // Prefixes ("schema" names) based on feed_id and feed_version get very messy, so we use random unique IDs.
            // We don't want to use an auto-increment numeric primary key because these need to be alphabetical.
            // Although ID collisions are theoretically possible, they are improbable in the extreme because our IDs
            // are long enough to have as much entropy as a UUID. So we don't really need to check for uniqueness and
            // retry in a loop.
            // TODO handle the case where we don't want any prefix.
            this.tablePrefix = this.namespace = randomIdString();
            result.uniqueIdentifier = tablePrefix;
            registerFeed();
            // Include the dot separator in the table prefix.
            // This allows everything to work even when there's no prefix.
            this.tablePrefix += ".";
            this.errorStorage = new SQLErrorStorage(connection, tablePrefix, true);
//            this.referenceTracker = new JdbcGtfsLoader.ReferenceTracker(errorStorage);
            // Load each table in turn, saving some summary information about what happened during each table load
            mapper = new ObjectMapper();
//            for (Agency agency : gtfsFeed.agency.values()) {
//                String sqlInsert = agency.toString();
//                insertStatement
//            }
            // Get entities from feeds.
            List<Calendar> calendars = gtfsFeed.services.values()
                    .stream()
                    .map(service -> service.calendar)
                    .collect(Collectors.toList());
            List<CalendarDate> calendarDates = gtfsFeed.services.values()
                    .stream()
                    .map(service -> service.calendar_dates.values())
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
            List<FareAttribute> fareAttributes = gtfsFeed.fares.values()
                    .stream()
                    .map(fare -> fare.fare_attribute)
                    .collect(Collectors.toList());
            List<FareRule> fareRules = gtfsFeed.fares.values()
                    .stream()
                    .map(fare -> fare.fare_rules)
                    .flatMap(List::stream)
                    .collect(Collectors.toList());
            List<Frequency> frequencies = gtfsFeed.frequencies.stream()
                    .map(stringFrequencyTuple2 -> stringFrequencyTuple2.b)
                    .collect(Collectors.toList());

            copyEntityToSql(gtfsFeed.agency.values(), Table.AGENCY);
            copyEntityToSql(calendars, Table.CALENDAR);
            copyEntityToSql(calendarDates, Table.CALENDAR_DATES);
            copyEntityToSql(gtfsFeed.routes.values(), Table.ROUTES);
            // FIXME
//            load(gtfsFeed.patterns.values(), Table.PATTERNS);
            copyEntityToSql(fareAttributes, Table.FARE_ATTRIBUTES);
            copyEntityToSql(fareRules, Table.FARE_RULES);
            copyEntityToSql(gtfsFeed.feedInfo.values(), Table.FEED_INFO);
            copyEntityToSql(gtfsFeed.shape_points.values(), Table.SHAPES);
            copyEntityToSql(gtfsFeed.stops.values(), Table.STOPS);
            copyEntityToSql(gtfsFeed.transfers.values(), Table.TRANSFERS);
            copyEntityToSql(gtfsFeed.trips.values(), Table.TRIPS); // refs routes
            copyEntityToSql(frequencies, Table.FREQUENCIES); // refs trips
            copyEntityToSql(gtfsFeed.stop_times.values(), Table.STOP_TIMES);
//            result.errorCount = errorStorage.getErrorCount();
            // This will commit and close the single connection that has been shared between all preceding load steps.
            errorStorage.commitAndClose();
            result.completionTime = System.currentTimeMillis();
            result.loadTimeMillis = result.completionTime - startTime;
            LOG.info("Loading tables took {} sec", result.loadTimeMillis / 1000);
        } catch (Exception ex) {
            // TODO catch exceptions separately while loading each table so load can continue, store in TableLoadResult
            LOG.error("Exception while loading GTFS file: {}", ex.toString());
            ex.printStackTrace();
            result.fatalException = ex.getMessage();
        }
        return result;
    }

    /**
     * Add a line to the list of loaded feeds showing that this feed has been loaded.
     * We used to inspect feed_info here so we could make our table prefix based on feed ID and version.
     * Now we just load feed_info like any other table.
     *         // Create a row in the table of loaded feeds for this feed
     * Really this is not just making the table prefix - it's loading the feed_info and should also calculate hashes.
     *
     * Originally we were flattening all feed_info files into one root-level table, but that forces us to drop any
     * custom fields in feed_info.
     */
    private void registerFeed () {

        // FIXME is this extra CSV reader used anymore? Check comment below.
        // First, inspect feed_info.txt to extract the ID and version.
        // We could get this with SQL after loading, but feed_info, feed_id and feed_version are all optional.
        FeedInfo feedInfo = gtfsFeed.feedInfo.isEmpty() ? null : gtfsFeed.feedInfo.values().iterator().next();
        String feedId = "", feedVersion = "";
        if (feedInfo != null) {
            feedId = feedInfo.feed_id;
            feedVersion = feedInfo.feed_version;
        }

        try {
            String md5Hex = null;
            String shaHex = null;
            Statement statement = connection.createStatement();
            // FIXME do the following only on databases that support schemas.
            // SQLite does not support them. Is there any advantage of schemas over flat tables?
            statement.execute("create schema " + tablePrefix);
            // current_timestamp seems to be the only standard way to get the current time across all common databases.
            // Record total load processing time?
            statement.execute("create table if not exists feeds (namespace varchar primary key, md5 varchar, " +
                    "sha1 varchar, feed_id varchar, feed_version varchar, filename varchar, loaded_date timestamp, " +
                    "snapshot_of varchar)");
            PreparedStatement insertStatement = connection.prepareStatement(
                    "insert into feeds values (?, ?, ?, ?, ?, ?, current_timestamp, null)");
            insertStatement.setString(1, tablePrefix);
            insertStatement.setString(2, md5Hex);
            insertStatement.setString(3, shaHex);
            // FIXME: will this throw an NPE if feedId or feedVersion are empty?
            insertStatement.setString(4, feedId.isEmpty() ? null : feedId);
            insertStatement.setString(5, feedVersion.isEmpty() ? null : feedVersion);
            insertStatement.setString(6, null);
            insertStatement.execute();
            connection.commit();
            LOG.info("Created new feed namespace: {}", insertStatement);
        } catch (Exception ex) {
            LOG.error("Exception while registering new feed namespace in feeds table: {}", ex.getMessage());
            DbUtils.closeQuietly(connection);
        }
    }

    private PreparedStatement getPreparedStatement(Table table) throws SQLException {
        table.createSqlTable(connection, namespace, true);
        String entityInsertSql = table.generateInsertSql(namespace, false);
        return connection.prepareStatement(entityInsertSql);
    }


    private <E extends Entity> void copyEntityToSql(Iterable<E> entities, Table table) throws SQLException {
        PreparedStatement insertStatement = getPreparedStatement(table);
        // Iterate over agencies and add to prepared statement
        for (E entity : entities) {
            entity.setStatementParameters(insertStatement);
            insertStatement.addBatch();
            // FIXME: Add batching execute on n
        }
        insertStatement.executeBatch();
    }

    /**
     * This wraps the main internal table loader method to catch exceptions and figure out how many errors happened.
     */
    private TableLoadResult load (Collection<? extends Entity> entities, Table table) {
        // This object will be returned to the caller to summarize the contents of the table and any errors.
        TableLoadResult tableLoadResult = new TableLoadResult();

        int initialErrorCount = errorStorage.getErrorCount();
        try {
            tableLoadResult.rowCount = loadInternal(entities, table);
        } catch (Exception ex) {
            LOG.error("Fatal error loading table", ex);
            tableLoadResult.fatalException = ex.toString();
            // Rollback connection so that fatal exception does not impact loading of other tables.
            try {
                connection.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } finally {
            // Explicitly delete the tmp file now that load is finished (either success or failure).
            // Otherwise these multi-GB files clutter the drive.
            if (tempTextFile != null) {
                tempTextFile.delete();
            }
        }
        int finalErrorCount = errorStorage.getErrorCount();
        tableLoadResult.errorCount = finalErrorCount - initialErrorCount;
        return tableLoadResult;
    }

    /**
     * This function will throw any exception that occurs. Those exceptions will be handled by the outer load method.
     * @return number of rows that were loaded.
     */
    private int loadInternal(Collection<? extends Entity> entities, Table table) throws Exception {
        if (entities == null || entities.size() == 0) {
            // This GTFS table could not be opened in the zip, even in a subdirectory.
            if (table.isRequired()) errorStorage.storeError(NewGTFSError.forTable(table, MISSING_TABLE));
            return 0;
        }
        LOG.info("Loading GTFS table {}", table.name);
        // Use the Postgres text load format if we're connected to that DBMS.
        boolean postgresText = (connection.getMetaData().getDatabaseProductName().equals("PostgreSQL"));

//        // Replace the GTFS spec Table with one representing the SQL table we will populate, with reordered columns.
        // FIXME this is confusing, we only create a new table object so we can call a couple of methods on it, all of which just need a list of fields.
        Table targetTable = new Table(tablePrefix + table.name, table.entityClass, table.required, table.fields);
//
//        // NOTE H2 doesn't seem to work with schemas (or create schema doesn't work).
//        // With bulk loads it takes 140 seconds to load the data and additional 120 seconds just to index the stop times.
//        // SQLite also doesn't support schemas, but you can attach additional database files with schema-like naming.
//        // We'll just literally prepend feed identifiers to table names when supplied.
//        // Some databases require the table to exist before a statement can be prepared.
        targetTable.createSqlTable(connection, true);




        // FIXME: This is using the string table writer.
        JdbcTableWriter tableWriter = new JdbcTableWriter(table, dataSource, this.namespace, connection);
        String entitiesString;
        try {
            entitiesString = mapper.writeValueAsString(entities);
            tableWriter.create(entitiesString, false);
            return 1;
        } catch (SQLException | IOException e) {
            e.printStackTrace();
            return 0;
        }


        //
//        // TODO are we loading with or without a header row in our Postgres text file?
//        if (postgresText) {
//            // No need to output headers to temp text file, our SQL table column order exactly matches our text file.
//            tempTextFile = File.createTempFile(targetTable.name, "text");
//            tempTextFileStream = new PrintStream(new BufferedOutputStream(new FileOutputStream(tempTextFile)));
//            LOG.info("Loading via temporary text file at " + tempTextFile.getAbsolutePath());
//        } else {
//            insertStatement = connection.prepareStatement(targetTable.generateInsertSql());
//            LOG.info(insertStatement.toString()); // Logs the SQL for the prepared statement
//        }
//
//        // When outputting text, accumulate transformed strings to allow skipping rows when errors are encountered.
//        // One extra position in the array for the CSV line number.
//        String[] transformedStrings = new String[table.fields.length + 1];
//
//        // Iterate over each record and prepare the record for storage in the table either through batch insert
//        // statements or postgres text copy operation.
//        int lineNumber = 1;
//        Iterator<? extends Entity> entityIterator = entities.iterator();
//        while (entityIterator.hasNext()) {
//            // FIXME: Use entity subclass
//            Object currentEntity = entityIterator.next();
//            // The CSV reader's current record is zero-based and does not include the header line.
//            // Convert to a CSV file line number that will make more sense to people reading error messages.
////            if (csvReader.getCurrentRecord() + 2 > Integer.MAX_VALUE) {
////                errorStorage.storeError(NewGTFSError.forTable(table, TABLE_TOO_LONG));
////                break;
////            }
//            lineNumber++;
//            if (lineNumber % 500_000 == 0) LOG.info("Processed {}", human(lineNumber));
////            if (csvReader.getColumnCount() != table.fields.length) {
////                String badValues = String.format("expected=%d; found=%d", table.fields.length, csvReader.getColumnCount());
////                errorStorage.storeError(NewGTFSError.forLine(table, lineNumber, WRONG_NUMBER_OF_FIELDS, badValues));
////                continue;
////            }
//            // Store value of key field for use in checking duplicate IDs
//            String keyValue = ((Entity) currentEntity).getId();
//            // The first field holds the line number of the CSV file. Prepared statement parameters are one-based.
//            if (postgresText) transformedStrings[0] = Integer.toString(lineNumber);
//            else insertStatement.setInt(1, lineNumber);
//            // FIXME: Use reflection here???
//            for (int f = 0; f < table.fields.length; f++) {
//                Field field = table.fields[f];
//                java.lang.reflect.Field currentField = table.entityClass.getField(field.name);
////                LOG.info(field.name);
//                Object value = currentField.get(currentEntity);
//                // Set value for field expects empty strings when null vales are present.
//                String string = value == null ? "" : value.toString();
//
//                // Use spec table to check that references are valid and IDs are unique.
//                // FIXME: Is there a need to track refs here? Probably.
//                if (referenceTracker != null) {
//                    table.checkReferencesAndUniqueness(keyValue, lineNumber, field, string, referenceTracker);
//                }
//                // Add value for entry into table
//                setValueForField(table, f, lineNumber, field, string, postgresText, transformedStrings);
//            }
//            if (postgresText) {
//                tempTextFileStream.printf(String.join("\t", transformedStrings));
//                tempTextFileStream.print('\n');
//            } else {
//                insertStatement.addBatch();
//                if (lineNumber % INSERT_BATCH_SIZE == 0) insertStatement.executeBatch();
//            }
//        }
//        // Record number is zero based but includes the header record, which we don't want to count.
//        // But if we are working with Postgres text file (without a header row) we have to add 1
//        // Iteration over all rows has finished, so We are now one record past the end of the file.
//        // FIXME: double check this.
//        int numberOfRecordsLoaded = lineNumber;
//        if (postgresText) {
//            numberOfRecordsLoaded = numberOfRecordsLoaded + 1;
//        }
//
//        // Finalize loading the table, either by copying the pre-validated text file into the database (for Postgres)
//        // or inserting any remaining rows (for all others).
//        if (postgresText) {
//            LOG.info("Loading into database table {} from temporary text file...", targetTable.name);
//            tempTextFileStream.close();
//            // Allows sending over network. This is only slightly slower than a local file copy.
//            final String copySql = String.format("copy %s from stdin", targetTable.name);
//            // FIXME we should be reading the COPY text from a stream in parallel, not from a temporary text file.
//            InputStream stream = new BufferedInputStream(new FileInputStream(tempTextFile.getAbsolutePath()));
//            // Our connection pool wraps the Connection objects, so we need to unwrap the Postgres connection interface.
//            CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
//            copyManager.copyIn(copySql, stream, 1024*1024);
//            stream.close();
//            // It is also possible to load from local file if this code is running on the database server.
//            // statement.execute(String.format("copy %s from '%s'", table.name, tempTextFile.getAbsolutePath()));
//        } else {
//            insertStatement.executeBatch();
//        }
//
//        targetTable.createIndexes(connection);
//
//        LOG.info("Committing transaction...");
//        connection.commit();
//        LOG.info("Done.");
//        return numberOfRecordsLoaded;
    }

    /**
     * Set value for a field either as a prepared statement parameter or (if using postgres text-loading) in the
     * transformed strings array provided. This also handles the case where the string is empty (i.e., field is null)
     * and when an exception is encountered while setting the field value (usually due to a bad data type), in which case
     * the field is set to null.
     */
    public void setValueForField(Table table, int fieldIndex, int lineNumber, Field field, String string, boolean postgresText, String[] transformedStrings) {
        if (string.isEmpty()) {
            // CSV reader always returns empty strings, not nulls
            if (field.isRequired() && errorStorage != null) {
                errorStorage.storeError(NewGTFSError.forLine(table, lineNumber, MISSING_FIELD, field.name));
            }
            setFieldToNull(postgresText, transformedStrings, fieldIndex, field);
        } else {
            // Micro-benchmarks show it's only 4-5% faster to call typed parameter setter methods
            // rather than setObject with a type code. I think some databases don't have setObject though.
            // The Field objects throw exceptions to avoid passing the line number, table name etc. into them.
            try {
                // FIXME we need to set the transformed string element even when an error occurs.
                // This means the validation and insertion step need to happen separately.
                // or the errors should not be signaled with exceptions.
                // Also, we should probably not be converting any GTFS field values.
                // We should be saving it as-is in the database and converting upon load into our model objects.
                if (postgresText) transformedStrings[fieldIndex + 1] = field.validateAndConvert(string);
                else field.setParameter(insertStatement, fieldIndex + 2, string);
            } catch (StorageException ex) {
                // FIXME many exceptions don't have an error type
                if (errorStorage != null) {
                    errorStorage.storeError(NewGTFSError.forLine(table, lineNumber, ex.errorType, ex.badValue));
                }
                // Set transformedStrings or prepared statement param to null
                setFieldToNull(postgresText, transformedStrings, fieldIndex, field);
            }
        }
    }

    /**
     * Sets field to null in statement or string array depending on whether postgres is being used.
     */
    private void setFieldToNull(boolean postgresText, String[] transformedStrings, int fieldIndex, Field field) {
        if (postgresText) transformedStrings[fieldIndex + 1] = POSTGRES_NULL_TEXT;
            // Adjust parameter index by two: indexes are one-based and the first one is the CSV line number.
        else try {
            //            LOG.info("setting {} index to null", fieldIndex + 2);
            insertStatement.setNull(fieldIndex + 2, field.getSqlType().getVendorTypeNumber());
        } catch (SQLException e) {
            e.printStackTrace();
            // FIXME: store error here? It appears that an exception should only be thrown if the type value is invalid,
            // the connection is closed, or the index is out of bounds. So storing an error may be unnecessary.
        }
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
                errorStorage.storeError(NewGTFSError.forFeed(COLUMN_NAME_UNSAFE, string));
            }
        }
        return clean;
    }
}
