package com.conveyal.gtfs.storage;

import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * Created by abyrd on 2017-03-27
 */
public class SqlLibrary {

    public Connection connection;

    private void example() throws Exception {
        String sql = "SELECT * FROM gtfs1234.stops";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setInt(1, 1000);
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next())
        {
            System.out.print("Column 1 returned ");
            System.out.println(resultSet.getString(1));
        }
        resultSet.close();
        preparedStatement.close();
    }

    public void executeSqlScript (String scriptName) {
        try {
            InputStream scriptStream = Backend.class.getResourceAsStream(scriptName);
            Scanner scanner = new Scanner(scriptStream).useDelimiter(";");
            while (scanner.hasNext()) {
                Statement currentStatement = connection.createStatement();
                currentStatement.execute(scanner.next());
            }
            scanner.close();
        } catch (Exception ex) {
            throw new StorageException(ex);
        }
    }

    /**
     * Read a bunch of SQL scripts out of a file on the classpath.
     * Each script begins with a comment (line beginning with two dashes).
     * The first whitespace-delimited token in that comment is the identifier of the script.
     * @param scriptName the resource to load on the classpath
     * @return a map from Strings to prepared statements for scripts
     */
    public Map<String, PreparedStatement> loadStatements (String scriptName) {
        Map<String, PreparedStatement> preparedStatements = new HashMap<>();
        try {
            InputStream scriptStream = Backend.class.getResourceAsStream(scriptName);
            Scanner statementScanner = new Scanner(scriptStream).useDelimiter(";");
            while (statementScanner.hasNext()) {
                String statement = statementScanner.next().trim();
                if (statement.startsWith("--")) {
                    Scanner tokenScanner = new Scanner(statement.substring(2));
                    if (tokenScanner.hasNext()) {
                        String firstToken = tokenScanner.next();
                        PreparedStatement preparedStatement = connection.prepareStatement(statement);
                        preparedStatements.put(firstToken, preparedStatement);
                    }
                }
            }
            statementScanner.close();
        } catch (Exception ex) {
            throw new StorageException(ex);
        }
        return preparedStatements;
    }


    /**
     * This will provide JDBC connections to components that need to execute SQL on the database server.
     * The JDBC in Java 7 and 8 support connection pooling.
     * "The key point with connection pooling is to avoid creating new connections where possible,
     * since it's usually an expensive operation. Reusing connections is critical for performance."
     * The JDBC DataSource is probably more appropriate than our custom connectionSource.
     * "Creating a new connection for each user can be time consuming (often requiring multiple seconds of clock time), in order to perform a database transaction that might take milliseconds."
     * http://commons.apache.org/proper/commons-dbcp/
     * There is a Tomcat DBCP which is a fork/repackage of Commons DBCP. It's not clear if they're substantially different.
     */
    public static DataSource createDataSource (String url) {
        // Connection factory will correctly handle null username and password
        ConnectionFactory connectionFactory =
                new DriverManagerConnectionFactory(url, null, null);
        PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null);
        GenericObjectPool connectionPool = new GenericObjectPool(poolableConnectionFactory);
        poolableConnectionFactory.setPool(connectionPool);
        // Fetches are super-slow with auto-commit turned on. Apparently it interferes with result cursors.
        // We also want auto-commit switched off for bulk inserts.
        poolableConnectionFactory.setDefaultAutoCommit(false);
        return new PoolingDataSource(connectionPool);
//        connection.setReadOnly();
//        connection.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT); // will this help? https://stackoverflow.com/a/18300252
    }

}
