package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.storage.StorageException;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import javax.xml.crypto.Data;
import java.sql.*;

/**
 * This will provide JDBC connections to components that need to execute SQL on the database server.
 * The JDBC in Java 7 and 8 support connection pooling.
 * "The key point with connection pooling is to avoid creating new connections where possible,
 * since it's usually an expensive operation. Reusing connections is critical for performance."
 * The JDBC DataSource is probably more appropriate than our custom connectionSource.
 * "Creating a new connection for each user can be time consuming (often requiring multiple seconds of clock time), in order to perform a database transaction that might take milliseconds."
 * http://commons.apache.org/proper/commons-dbcp/
 *
 * There is a Tomcat DBCP which is a fork/repackage of Commons DBCP. It's not clear if they're substantially different.
 *
 * TODO allow configuring this with an arbitrary URL
 */
public class ConnectionSource {

    public static final String H2_FILE_URL = "jdbc:h2:file:~/test-db"; // H2 memory does not seem faster than file
    public static final String SQLITE_FILE_URL = "jdbc:sqlite:/Users/abyrd/test-db";
    public static final String POSTGRES_LOCAL_URL = "jdbc:postgresql://localhost/catalogue";

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionSource.class);

    String url;

    private static DataSource dataSource = null;

    private static DataSource getDataSource () {
        if (dataSource == null) {
            LOG.info("Setting up database connection pool.");
            // Connection factory will correctly handle null username and password
            ConnectionFactory connectionFactory =
                    new DriverManagerConnectionFactory(POSTGRES_LOCAL_URL, null, null);
            PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null);
            GenericObjectPool connectionPool = new GenericObjectPool(poolableConnectionFactory);
            poolableConnectionFactory.setPool(connectionPool);
            // Fetches are super-slow with auto-commit turned on. Apparently it interferes with result cursors.
            // We also want auto-commit switched off for bulk inserts.
            poolableConnectionFactory.setDefaultAutoCommit(false);
            dataSource = new PoolingDataSource(connectionPool);
        }
        return dataSource;
    }

    public static Connection getConnection () {

//        connection.setReadOnly();
//        connection.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT); // will this help? https://stackoverflow.com/a/18300252

        try {
            return getDataSource().getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
