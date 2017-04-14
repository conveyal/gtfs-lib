package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.storage.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

import static com.conveyal.gtfs.loader.CsvLoader.sanitize;

/**
 * This will provide JDBC connections to components that need to execute SQL on the database server.
 * We need to be able to generate or re-generate multiple connections to the same server.
 * Created by abyrd on 2017-04-06
 */
public class ConnectionSource {

    public static final String H2_FILE_URL = "jdbc:h2:file:~/test-db"; // H2 memory does not seem faster than file
    public static final String SQLITE_FILE_URL = "jdbc:sqlite:/Users/abyrd/test-db";
    public static final String POSTGRES_LOCAL_URL = "jdbc:postgresql://localhost/catalogue";

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionSource.class);

    String url;

    /**
     * @param url the JDBC connection URL. (could add schema here too but it's not standard)
     */
    public ConnectionSource (String url) {
        this.url = url;
    }

    /**
     * @param schema the unique identifier of the GTFS feed version that you want to access.
     *               This will be prefixed on the GTFS table names. If null, uses the public schema.
     */
    public Connection getConnection (String schema) {
        // JODBC drivers should auto-register these days. You used to have to trick the class loader into loading them.
        try {
            Connection connection = DriverManager.getConnection(url);
            if (schema != null) {
                Statement statement = connection.createStatement();
                String sql = String.format("create schema if not exists %s", sanitize(schema));
                LOG.info(sql);
                statement.execute(sql);
                // We could also just prefix the table names with the feedVersionId and a dot, but this is a bit cleaner.
                connection.setSchema(schema);
            }
            // Fetches are super-slow with auto-commit turned on. Apparently it interferes with result cursors.
            // We also want auto-commit switched off for bulk inserts.
            connection.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
            connection.setAutoCommit(false);
            return connection;
        } catch (SQLException ex) {
            throw new StorageException(ex);
        }
    }

}
