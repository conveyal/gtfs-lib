package com.conveyal.gtfs;

import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.loader.JdbcGtfsLoader;
import org.apache.commons.cli.*;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.sql.DataSource;

/**
 * This is the public interface to the RDBMS backed functionality in gtfs-lib.
 * To avoid coupling our other projects too closely to the internal implementation of GTFS-lib, we should only
 * use GTFS-lib via methods in this class and the data model objects in the model package.
 */
public abstract class GTFS {

    private static final Logger LOG = LoggerFactory.getLogger(GTFS.class);

    private static final String DEFAULT_DATABASE_URL = "jdbc:postgresql://localhost/gtfs";

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
    public static String load (String filePath, DataSource dataSource) {
        JdbcGtfsLoader loader = new JdbcGtfsLoader(filePath, dataSource);
        String uniqueId = loader.loadTables();
        return uniqueId;
    }

    public static void validate (String feedId, DataSource dataSource) {
        Feed feed = new Feed(dataSource, feedId);
        feed.validate();
        feed.close();
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
        // ConnectionFactory can handle null username and password (for local host-based authentication)
        ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(url, username, password);
        PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null);
        GenericObjectPool connectionPool = new GenericObjectPool(poolableConnectionFactory);
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
    public static void main (String[] args) {

        Options options = getOptions();
        CommandLine cmd = null;
        try {
            cmd = new DefaultParser().parse(options, args);
        } catch (ParseException e) {
            LOG.error("Error parsing command line: " + e.getMessage());
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

        if (!(cmd.hasOption("load") || cmd.hasOption("validate") || cmd.hasOption("graphql"))) {
            LOG.error("Must specify one of 'load', 'validate', or 'graphql'.");
            printHelp(options);
            return;
        }

        String databaseUrl = cmd.getOptionValue("database", DEFAULT_DATABASE_URL);
        String databaseUser = cmd.getOptionValue("user");
        String databasePassword = cmd.getOptionValue("password");

        // Create a JDBC connection pool for the specified database.
        // Missing (null) username and password will fall back on host-based authentication.
        DataSource dataSource = createDataSource(databaseUrl, databaseUser, databasePassword);

        // Record the unique identifier of the newly loaded feed
        String loadedFeed = null;
        if (cmd.hasOption("load")) {
            String filePath = cmd.getOptionValue("load");
            loadedFeed = load(filePath, dataSource);
            LOG.info("The unique identifier for this feed is: {}", loadedFeed);
        }

        if (cmd.hasOption("validate")) {
            String feedToValidate = cmd.getOptionValue("validate");
            if (feedToValidate != null && loadedFeed != null) {
                LOG.warn("Validating the specified feed {} instead of {} (just loaded)", feedToValidate, loadedFeed);
            }
            if (feedToValidate == null && loadedFeed != null) {
                feedToValidate = loadedFeed;
            }
            if (feedToValidate != null) {
                LOG.info("Validating feed with unique identifier {}", feedToValidate);
                validate (feedToValidate, dataSource);
            } else {
                LOG.error("No feed to validate. Specify one, or load a feed in the same command.");
            }
        }

        if (cmd.hasOption("graphql")) {
            Integer port = Integer.parseInt(cmd.getOptionValue("graphql"));
            LOG.info("Starting GraphQL server on port {}", port);
            throw new UnsupportedOperationException();
        }

    }

    /**
     * The parameter to Option.builder is the short option. Use the no-arg builder constructor with .longOpt() to
     * specify an option that has no short form.
     */
    private static Options getOptions () {
        Options options = new Options();
        options.addOption(Option.builder("h").longOpt("help").desc("print this message").build());
        options.addOption(Option.builder().longOpt("load").hasArg()
                .argName("file").desc("load GTFS data from the given file").build());
        options.addOption(Option.builder().longOpt("validate").hasArg().optionalArg(true).argName("feed")
                .desc("validate the specified feed. defaults to the feed loaded with the --load option").build());
        options.addOption(Option.builder("d").longOpt("database")
                .hasArg().argName("url").desc("JDBC URL for the database. Defaults to " + DEFAULT_DATABASE_URL).build());
        options.addOption(Option.builder("u").longOpt("user")
                .hasArg().argName("username").desc("database username").build());
        options.addOption(Option.builder("p").longOpt("password")
                .hasArg().argName("password").desc("database password").build());
        options.addOption(Option.builder().longOpt("graphql")
                .desc("start a GraphQL API on the given port").optionalArg(true).build());
        options.addOption(Option.builder().longOpt("json").desc("optionally store in result.json").build());
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
