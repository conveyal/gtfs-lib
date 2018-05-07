package com.conveyal.gtfs.error;

import com.conveyal.gtfs.storage.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Map;

/**
 * This is an abstraction for something that stores GTFS loading and validation errors one by one.
 * Currently there's only one implementation, which uses SQL tables.
 * We used to store the errors in plain old Lists, and could make an alternative implementation to do so.
 * We may need to in order to output JSON reports.
 */
public class SQLErrorStorage {

    private static final Logger LOG = LoggerFactory.getLogger(SQLErrorStorage.class);

    // TODO Look into pooling prepared statements.

    // It is debatable whether we should be holding a single connection from a pool open.
    // Fetching a pooled connection might slow things down in sections where many thousands of errors are saved.
    // By reusing the exact same connection as the GTFS table loader, we ensure that the newly created schema is
    // visible to the connection when it creates the error tables, but we have to be careful where in the code we
    // record errors.
    private Connection connection;

    private PreparedStatement insertError;
    private PreparedStatement insertInfo;

    // A string to prepend to all table names. This is a unique identifier for the particular feed that is being loaded.
    // Should include any dot or other separator. May also be the empty string if you want no prefix added.
    private String tablePrefix;

    // How many errors to insert at a time in a batch, for efficiency.
    private static final long INSERT_BATCH_SIZE = 500;

    public SQLErrorStorage (Connection connection, String tablePrefix, boolean createTables) {
        // TablePrefix should always be internally generated so doesn't need to be sanitized.
        this.tablePrefix = tablePrefix == null ? "" : tablePrefix;
        this.connection = connection;
        if (createTables) createErrorTables();
        createPreparedStatements();
    }

    public void storeError (NewGTFSError error) {
        try {
            // Insert one row for the error itself
            // NOTE: The error ID is a serial auto-incrementing value, so it does not need to be set in the statement.
            insertError.setString(1, error.errorType.name());
            // Using SetObject to allow null values, do all target DBs support this?
            insertError.setObject(2, error.entityType == null ? null : error.entityType.getSimpleName());
            insertError.setObject(3, error.lineNumber);
            insertError.setObject(4, error.entityId);
            insertError.setObject(5, error.entitySequenceNumber);
            insertError.setObject(6, error.badValue);
            insertError.execute();
            // Get the error ID from the insert to use for reference in the following error info inserts.
            ResultSet generatedKeys = insertError.getGeneratedKeys();
            generatedKeys.next();
            int errorId = (int) generatedKeys.getLong(1);
            // Insert all key-value info pairs for the error
            int errorInfoCount = 0;
            for (Map.Entry<String, String> entry : error.errorInfo.entrySet()) {
                insertInfo.setInt(1, errorId);
                insertInfo.setString(2, entry.getKey());
                insertInfo.setString(3, entry.getValue());
                insertInfo.addBatch();
                errorInfoCount++;
            }
            if (errorInfoCount % INSERT_BATCH_SIZE == 0) {
                insertInfo.executeBatch();
            }
        } catch (SQLException ex) {
            throw new StorageException(ex);
        }
    }

    public int getErrorCount () {
        try {
            Statement statement = connection.createStatement();
            statement.execute(String.format("select count(*) from %serrors", tablePrefix));
            ResultSet resultSet = statement.getResultSet();
            resultSet.next();
            int count = resultSet.getInt(1);
            LOG.info("Error count is {} for {}", count, tablePrefix);
            return count;
        } catch (SQLException ex) {
            throw new StorageException(ex);
        }
    }

    /**
     * This commits any remaining inserts, commits the transaction, and closes the connection permanently.
     * commitAndClose() should only be called when access to SQLErrorStorage is no longer needed.
     */
    public void commitAndClose() {
        try {
            // Execute any remaining batch inserts and commit the transaction.
            insertError.executeBatch();
            insertInfo.executeBatch();
            connection.commit();
            // Close the connection permanently (should be called only after errorStorage instance no longer needed).
            connection.close();
        } catch (SQLException ex) {
            throw new StorageException(ex);
        }
    }

    private void createErrorTables() {
        try {
            Statement statement = connection.createStatement();
            // If tables are dropped, order matters because of foreign keys.
            // TODO add foreign key constraint on info table?
            String createErrorsSql = String.format("create table %serrors (error_id serial primary key, error_type varchar, " +
                    "entity_type varchar, line_number integer, entity_id varchar, entity_sequence integer, " +
                    "bad_value varchar)", tablePrefix);
            LOG.info(createErrorsSql);
            statement.execute(createErrorsSql);
            String createErrorInfoSql = String.format("create table %serror_info (error_id integer, key varchar, value varchar)",
                    tablePrefix);
            LOG.info(createErrorInfoSql);
            statement.execute(createErrorInfoSql);
            connection.commit();
            // Keep connection open, closing would null the wrapped connection and return it to the pool.
        } catch (SQLException ex) {
            throw new StorageException(ex);
        }
    }

    private void createPreparedStatements () {
        try {
            insertError = connection.prepareStatement(
                    // Insert error does not set the error ID because it is auto-incrementing per the table defintion.
                    String.format("insert into %serrors values (DEFAULT, ?, ?, ?, ?, ?, ?)", tablePrefix),
                    Statement.RETURN_GENERATED_KEYS);
            insertInfo = connection.prepareStatement(
                    String.format("insert into %serror_info values (?, ?, ?)", tablePrefix));
        } catch (SQLException ex) {
            throw new StorageException(ex);
        }
    }

}
