package com.conveyal.gtfs;

import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.loader.FeedLoadResult;
import com.conveyal.gtfs.loader.JdbcGtfsExporter;
import com.conveyal.gtfs.loader.JdbcGtfsLoader;
import com.conveyal.gtfs.loader.JdbcGtfsSnapshotter;
import com.conveyal.gtfs.loader.SnapshotResult;
import com.conveyal.gtfs.util.InvalidNamespaceException;
import com.conveyal.gtfs.validator.FeedValidatorCreator;
import com.conveyal.gtfs.validator.ValidationResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;
import org.apache.commons.cli.*;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.function.BiFunction;

import static com.conveyal.gtfs.util.Util.ensureValidNamespace;

/**
 * This is the public interface to the RDBMS backed functionality in gtfs-lib.
 * To avoid coupling our other projects too closely to the internal implementation of GTFS-lib, we should only
 * use GTFS-lib via methods in this class and the data model objects in the model package.
 */
public abstract class GTFS {

    private static final Logger LOG = LoggerFactory.getLogger(GTFS.class);

    private static final String DEFAULT_DATABASE_URL = "jdbc:postgresql://localhost/gtfs";

    /**
     * Export a feed ID from the database to a zipped GTFS file in the specified export directory.
     */
    public static FeedLoadResult export (String feedId, String outFile, DataSource dataSource, boolean fromEditor) {
        JdbcGtfsExporter exporter = new JdbcGtfsExporter(feedId, outFile, dataSource, fromEditor);
        FeedLoadResult result = exporter.exportTables();
        return result;
    }

    /**
     * Load the GTFS data in the specified file into the given JDBC DataSource.
     *
     * About loading from an object on S3:
     * We've got a problem with keeping the identifier on S3 the same as the identifier in the database.
     * Question: do we really want to load the feed from S3 or do we just want to move the local file to S3 when done?
     * i.e. user uploads to the datatools back end,
     * The advantage of having the end users upload to s3, then is that only the files currently being processed need
     * to be held on our backend server, not the whole queue waiting to be processed.
     * But I think currently we have some hack where the end user gets a token to upload to S3, which seems bad.
     * Probably we should have end users upload directly to the backend server, which then loads the feed into RDBMS,
     * uploads the file to S3 with the unique name supplied by gtfs-lib, and deletes the local file.
     *
     * @return a unique identifier for the newly loaded feed in the database
     */
    public static FeedLoadResult load (String filePath, DataSource dataSource) {
        JdbcGtfsLoader loader = new JdbcGtfsLoader(filePath, dataSource);
        FeedLoadResult result = loader.loadTables();
        return result;
    }

    /**
     * Copy all tables for a given feed ID (schema namespace) into a new namespace in the given JDBC DataSource.
     *
     * The resulting snapshot from this operation is intended to be edited, so there are a handful of changes made to
     * the newly copied tables:
     *   1. The tables' id column has been modified to be auto-incrementing.
     *   2. Primary keys may be added to certain columns/tables.
     *   3. Additional editor-specific columns are added to certain tables.
     * @param feedId        feed ID (schema namespace) to copy from
     * @param dataSource    JDBC connection to existing database
     * @return              FIXME should this be a separate SnapshotResult object?
     */
    public static SnapshotResult makeSnapshot (String feedId, DataSource dataSource) {
        JdbcGtfsSnapshotter snapshotter = new JdbcGtfsSnapshotter(feedId, dataSource);
        SnapshotResult result = snapshotter.copyTables();
        return result;
    }

    /**
     * Once a feed has been loaded into the database, examine its contents looking for various problems and errors.
     */
    public static ValidationResult validate (String feedId, DataSource dataSource, FeedValidatorCreator... additionalValidators) {
        Feed feed = new Feed(dataSource, feedId);
        ValidationResult result = feed.validate(additionalValidators);
        return result;
    }

    /**
     * Deletes all tables for the specified feed. Simply put, this is a "drop schema" SQL statement called on the feed's
     * namespace.
     */
    public static void delete (String feedId, DataSource dataSource) throws SQLException, InvalidNamespaceException {
        LOG.info("Deleting all tables (dropping schema) for {} feed namespace.", feedId);
        // Try-with-resources will automatically close the connection when the try block exits.
        try (Connection connection = dataSource.getConnection()) {
            ensureValidNamespace(feedId);
            // Mark entry in feeds table as deleted.
            String deleteFeedEntrySql = "update feeds set deleted = true where namespace = ?";
            PreparedStatement deleteFeedStatement = connection.prepareStatement(deleteFeedEntrySql);
            deleteFeedStatement.setString(1, feedId);
            deleteFeedStatement.executeUpdate();
            String schemaSql = String.format(
                "SELECT schema_name FROM information_schema.schemata where schema_name = '%s'",
                feedId
            );
            PreparedStatement preparedStatement = connection.prepareStatement(schemaSql);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                LOG.info("schema + " + resultSet.getString("schema_name"));
            }
            // Drop all tables bearing the feedId namespace.
            // Note: It does not appear to be possible to use prepared statements with "drop schema."
            String dropSchemaSql = String.format("drop schema %s cascade;", feedId);
            PreparedStatement dropSchemaStatement = connection.prepareStatement(dropSchemaSql);
            LOG.info(dropSchemaStatement.toString());
            dropSchemaStatement.executeUpdate();
            // Commit the changes.
            connection.commit();
            LOG.info("Drop schema successful!");
        } catch (InvalidNamespaceException | SQLException e) {
            LOG.error(String.format("Could not drop feed for namespace %s", feedId), e);
            throw e;
        }
    }

    /**
     * Create an automatically managed pool of database connections to the supplied JDBC database URL.
     *
     * Creating new database connections is usually an expensive operation,
     * taking up to seconds to prepare for a transaction that only lasts milliseconds.
     * The JDBC in Java 7 and 8 support connection pooling, which is critical for performance.
     * We are using the standard JDBC DataSource interface, with the Apache commons-dbcp implementation.
     * This should be roughly the same connection pooling used in the Tomcat application server.
     *
     * Here are some sample database URLs to serve as patterns:
     * H2_FILE_URL = "jdbc:h2:file:~/test-db"; // H2 memory does not seem faster than H2 file
     * SQLITE_FILE_URL = "jdbc:sqlite:/Users/abyrd/test-db";
     * POSTGRES_LOCAL_URL = "jdbc:postgresql://localhost/catalogue";
     *
     * For local Postgres connections, you can supply a null username and password to use host-based authentication.
     */
    public static DataSource createDataSource (String url, String username, String password) {
        String characterEncoding = Charset.defaultCharset().toString();
        LOG.debug("Default character encoding: {}", characterEncoding);
        if (!Charset.defaultCharset().equals(StandardCharsets.UTF_8)) {
            // Character encoding must be set to UTF-8 in order for the database connection to work without error.
            // To override default encoding at runtime, run application jar with encoding environment variable set to
            // UTF-8 (or update IDE settings). TODO we should also check that JDBC and the database know to use UTF-8.
            throw new RuntimeException("Your system's default encoding (" + characterEncoding + ") is not supported. Please set it to UTF-8. Example: java -Dfile.encoding=UTF-8 application.jar");
        }
        // ConnectionFactory can handle null username and password (for local host-based authentication)
        ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(url, username, password);
        PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null);
        GenericObjectPool connectionPool = new GenericObjectPool(poolableConnectionFactory);
        // TODO: set other options on connectionPool?
        connectionPool.setMaxTotal(300);
        connectionPool.setMaxIdle(4);
        connectionPool.setMinIdle(2);
        poolableConnectionFactory.setPool(connectionPool);
        // We also want auto-commit switched off for bulk inserts, and also because fetches are super-slow with
        // auto-commit turned on. Apparently it interferes with result cursors.
        poolableConnectionFactory.setDefaultAutoCommit(false);
        return new PoolingDataSource(connectionPool);
        // We might want already-loaded feeds to be treated as read-only.
        // But we need to call this on the connection, not the connectionSource.
        // connection.setReadOnly();
        // Not sure we need to close cursors - closing the pool-wrapped connection when we're done with it should also close cursors.
        // will this help? https://stackoverflow.com/a/18300252
        // connection.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    /**
     * A command-line interface that lets you load GTFS feeds into a database and validate the loaded feeds.
     * It also lets you run a GraphQL API for all the feeds loaded into the database.
     */
    public static void main (String[] args) throws IOException {
        Options options = getOptions();
        CommandLine cmd;
        try {
            cmd = new DefaultParser().parse(options, args);
        } catch (ParseException e) {
            LOG.error("Error parsing command line", e);
            printHelp(options);
            return;
        }

        if (cmd.hasOption("help")) {
            printHelp(options);
            return;
        }

        if (!cmd.getArgList().isEmpty()) {
            LOG.error("Extraneous arguments present: {}", cmd.getArgs());
            printHelp(options);
            return;
        }

        if (!(cmd.hasOption("export") || cmd.hasOption("snapshot") || cmd.hasOption("load") || cmd.hasOption("validate") || cmd.hasOption("delete"))) {
            LOG.error("Must specify one of 'snapshot', 'load', 'validate', 'export', or 'delete'.");
            printHelp(options);
            return;
        }
        boolean storeResults = cmd.hasOption("json");
        // Object mapper and directory used for writing load or validation results to file if required.
        ObjectMapper mapper = null;
        File directory = null;
        if (storeResults) {
            // Instantiate mapper for use with outputting load/validation results.
            mapper = new ObjectMapper();
            directory = cmd.getOptionValue("json") != null ? new File(cmd.getOptionValue("json")) : Files.createTempDir();
            LOG.info("Storing results in directory: {}", directory.getAbsolutePath());
        }
        String databaseUrl = cmd.getOptionValue("database", DEFAULT_DATABASE_URL);
        String databaseUser = cmd.getOptionValue("user");
        String databasePassword = cmd.getOptionValue("password");
        LOG.info("Connecting to {} as user {}", databaseUrl, databaseUser);

        // Create a JDBC connection pool for the specified database.
        // Missing (null) username and password will fall back on host-based authentication.
        DataSource dataSource = createDataSource(databaseUrl, databaseUser, databasePassword);

        // Record the unique identifier of the newly loaded feed
        FeedLoadResult loadResult = null;
        if (cmd.hasOption("load")) {
            String filePath = cmd.getOptionValue("load");
            loadResult = load(filePath, dataSource);
            if (storeResults) {
                File loadResultFile = new File(directory, String.format("%s-load.json", loadResult.uniqueIdentifier));
                LOG.info("Storing load result at {}", loadResultFile.getAbsolutePath());
                mapper.writerWithDefaultPrettyPrinter().writeValue(loadResultFile, loadResult);
            }
            LOG.info("The unique identifier for this feed is: {}", loadResult.uniqueIdentifier);
        }

        if (cmd.hasOption("validate")) {
            String feedToValidate = cmd.getOptionValue("validate");
            if (feedToValidate != null && loadResult != null) {
                LOG.warn("Validating the specified feed {} instead of {} (just loaded)",
                        feedToValidate, loadResult.uniqueIdentifier);
            }
            if (feedToValidate == null && loadResult != null) {
                feedToValidate = loadResult.uniqueIdentifier;
            }
            if (feedToValidate != null) {
                LOG.info("Validating feed with unique identifier {}", feedToValidate);
                ValidationResult validationResult = validate (feedToValidate, dataSource);
                if (storeResults) {
                    File validationResultFile = new File(directory, String.format("%s-validation.json", feedToValidate));
                    LOG.info("Storing validation result at {}", validationResultFile.getAbsolutePath());
                    mapper.writerWithDefaultPrettyPrinter().writeValue(validationResultFile, validationResult);
                }
                LOG.info("Done validating.");
            } else {
                LOG.error("No feed to validate. Specify one, or load a feed in the same command.");
            }
        }

        if (cmd.hasOption("snapshot")) {
            String namespaceToSnapshot = cmd.getOptionValue("snapshot");
            if (namespaceToSnapshot == null && loadResult != null) {
                namespaceToSnapshot = loadResult.uniqueIdentifier;
            }
            if (namespaceToSnapshot != null) {
                LOG.info("Snapshotting feed with unique identifier {}", namespaceToSnapshot);
                FeedLoadResult snapshotResult = makeSnapshot(namespaceToSnapshot, dataSource);
                if (storeResults) {
                    File snapshotResultFile = new File(directory, String.format("%s-snapshot.json", snapshotResult.uniqueIdentifier));
                    LOG.info("Storing validation result at {}", snapshotResultFile.getAbsolutePath());
                    mapper.writerWithDefaultPrettyPrinter().writeValue(snapshotResultFile, snapshotResult);
                }
                LOG.info("Done snapshotting. The unique identifier for this snapshot is: {}", snapshotResult.uniqueIdentifier);
            } else {
                LOG.error("No feed to snapshot. Specify one, or load a feed in the same command.");
            }
        }

        if (cmd.hasOption("export")) {
            String namespaceToExport = cmd.getOptionValue("export");
            String outFile = cmd.getOptionValue("outFile");
            if (namespaceToExport == null && loadResult != null) {
                namespaceToExport = loadResult.uniqueIdentifier;
            }
            if (namespaceToExport != null) {
                LOG.info("Exporting feed with unique identifier {}", namespaceToExport);
                FeedLoadResult exportResult = export(namespaceToExport, outFile, dataSource, true);
                LOG.info("Done exporting.");
            } else {
                LOG.error("No feed to export. Specify one, or load a feed in the same command.");
            }
        }

        if (cmd.hasOption("delete")) {
            String namespaceToDelete = cmd.getOptionValue("delete");

            if (namespaceToDelete != null) {
                LOG.info("Deleting feed with unique identifier {}", namespaceToDelete);
                try {
                    delete(namespaceToDelete, dataSource);
                    LOG.info("Feed {} has been successfully deleted.", namespaceToDelete);
                } catch (SQLException | InvalidNamespaceException e) {
                    e.printStackTrace();
                }
            } else {
                LOG.error("No feed to delete. Specify one with the --delete argument.");
            }
        }
    }

    /**
     * The parameter to Option.builder is the short option. Use the no-arg builder constructor with .longOpt() to
     * specify an option that has no short form.
     */
    private static Options getOptions () {
        Options options = new Options();
        options.addOption(Option.builder("h").longOpt("help").desc("print this message").build());
        options.addOption(Option.builder()
                .longOpt("export").hasArg()
                .argName("namespace")
                .desc("export GTFS data from the given database namespace (feed) to the given directory").build());
        options.addOption(Option.builder()
                .longOpt("outFile").hasArg()
                .argName("file")
                .desc("zip file path for the exported GTFS").build());
        options.addOption(Option.builder()
                .longOpt("load").hasArg()
                .argName("file")
                .desc("load GTFS data from the given file").build());
        options.addOption(Option.builder()
                .longOpt("validate").hasArg().optionalArg(true)
                .argName("namespace")
                .desc("validate the specified feed. defaults to the feed loaded with the --load option").build());
        options.addOption(Option.builder()
                .longOpt("snapshot").hasArg()
                .argName("namespace")
                .desc("snapshot GTFS data from the given database namespace (feed)").build());
        options.addOption(Option.builder("d")
                .longOpt("database").hasArg()
                .argName("url")
                .desc("JDBC URL for the database. Defaults to " + DEFAULT_DATABASE_URL).build());
        options.addOption(Option.builder("u").longOpt("user").hasArg()
                .argName("username")
                .desc("database username").build());
        options.addOption(Option.builder("p")
                .longOpt("password").hasArg()
                .argName("password")
                .desc("database password").build());
        options.addOption(Option.builder()
                .longOpt("delete").hasArg()
                .argName("namespace")
                .desc("delete the feed for the specified namespace.").build());
        options.addOption(Option.builder()
                .longOpt("json").hasArg().optionalArg(true)
                .argName("directory")
                .desc("optionally store results in specified directory (defaults to system temp)").build());
        return options;
    }

    private static void printHelp(Options options) {
        final String HELP = String.join("\n",
                "java -cp gtfs-lib-shaded.jar com.conveyal.gtfs.GTFS [options]",
                // blank lines for legibility
                "",
                ""
        );
        HelpFormatter formatter = new HelpFormatter();
        System.out.println(); // blank line for legibility
        formatter.printHelp(HELP, options);
        System.out.println(); // blank line for legibility
    }


}
