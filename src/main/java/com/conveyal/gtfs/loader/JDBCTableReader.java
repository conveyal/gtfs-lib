package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.model.Entity;
import com.conveyal.gtfs.storage.StorageException;
import com.vividsolutions.jts.awt.PointShapeFactory;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Iterator;

import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;

/**
 * This wraps a single database table and provides methods to load its rows into Java objects, iterate over the
 * contents of the table, and select individual rows.
 *
 * Created by abyrd on 2017-04-06
 */
public class JDBCTableReader<T extends Entity> implements TableReader<T> {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCTableReader.class);

    // See https://www.postgresql.org/docs/9.6/static/errcodes-appendix.html
    public static final String SQL_STATE_UNDEFINED_TABLE = "42P01";

    private final Table specTable;
    private final EntityPopulator<T> entityPopulator;

    private final TObjectIntMap<String> columnForName;
    private final DataSource dataSource;
    private final String qualifiedTableName;
    private final String selectClause;
    /**
     * @param tablePrefix must not be null, can be empty string, should include any separator character (dot)
     */
    public JDBCTableReader(Table specTable, DataSource dataSource, String tablePrefix, EntityPopulator<T> entityPopulator) {
        qualifiedTableName = tablePrefix + specTable.name;
        this.dataSource = dataSource;
        this.entityPopulator = entityPopulator;
        this.specTable = specTable;
        // Prepare a mapping from column names to indexes. This allows us to avoid throwing exceptions on missing columns.
        // We do this in the constructor to avoid rebuilding the mapping every time we fetch a single entity from the table.
        // No entry value defaults to zero, and SQL columns are 1-based.
        columnForName = new TObjectIntHashMap<>();
        selectClause = "select * from " + qualifiedTableName;
        // Try-with-resources will automatically close the connection when the try block exits.
        try (Connection connection = dataSource.getConnection()) {
            LOG.info("Connected to {}", qualifiedTableName);
            PreparedStatement selectAll = connection.prepareStatement(
                    selectClause, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, CLOSE_CURSORS_AT_COMMIT);
            ResultSetMetaData metaData = selectAll.getMetaData();
            int nColumns = metaData.getColumnCount();
            for (int c = 1; c <= nColumns; c++) {
                columnForName.put(metaData.getColumnName(c), c);
            }
        } catch (SQLException e) {
            if (specTable.isRequired()) {
                LOG.warn("Could not connect to required table " + qualifiedTableName);
            }
        }
    }

    /**
     * As a convenience, the TableReader itself is iterable.
     * Seen as an iterable, the TableReader is equivalent to calling tableReader.getAll().
     */
    @Override
    public Iterator<T> iterator() {
        return this.getAll().iterator();
    }

    /**
     * Get a single item from this table by ID.
     */
    @Override
    public T get (final String id) {
        // This is slightly less efficient than writing custom code, but code reuse is good.
        return getUnordered(id).iterator().next();
    }

    /**
     * Get all the items from this table with the given ID, in order.
     */
    @Override
    public Iterable<T> getOrdered (final String id) {
        // An iterable has a single method that produces an iterator.
        return () -> { return new EntityIterator(id, true); };
    }

    /**
     * Get all the items from this table with the given ID, in unspecified order.
     */
    public Iterable<T> getUnordered (final String id) {
        // An iterable has a single method that produces an iterator.
        return () -> { return new EntityIterator(id, false); };
    }

    /**
     * Get all the items from this table in an unspecified order.
     */
    @Override
    public Iterable<T> getAll () {
        // An iterable has a single method that produces an iterator.
        return () -> { return new EntityIterator(null, false); };
    }

    /**
     * Get all the items from this table in an unspecified order.
     */
    @Override
    public Iterable<T> getAllOrdered () {
        // An iterable has a single method that produces an iterator.
        return () -> { return new EntityIterator(null, true); };
    }

    /**
     * @return the total number of rows in this table, or -1 if the table does not exist.
     */
    public int getRowCount() {
        try {
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
            statement.execute("select count(*) from " + qualifiedTableName);
            ResultSet resultSet = statement.getResultSet();
            resultSet.next();
            int nRows = resultSet.getInt(1);
            connection.close();
            return nRows;
        } catch (SQLException ex) {
            if (SQL_STATE_UNDEFINED_TABLE.equals(ex.getSQLState())) {
                // Table is missing, signal this to the caller.
                return -1;
            } else {
                throw new StorageException(ex);
            }
        }
    }

    private class EntityIterator implements Iterator<T> {

        private Connection connection; // Will remain open for the duration of the iteration.
        private boolean hasMoreEntities;
        private ResultSet results;

        EntityIterator (String id, boolean ordered) {
            try {
                connection = dataSource.getConnection();
                PreparedStatement preparedStatement;
                String sql = selectClause;
                String idField = specTable.getKeyFieldName();
                String orderByField = specTable.getOrderFieldName();
                if (id != null) {
                    sql += String.format(" where %s = ?", idField);
                }
                if (ordered && orderByField != null) {
                    sql += String.format(" order by %s, %s", idField, orderByField);
                }
                preparedStatement =
                        connection.prepareStatement(sql, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, CLOSE_CURSORS_AT_COMMIT);
                if (id != null && orderByField == null) {
                    // Select a particular ID on a table without sequence numbers. There should be only one result.
                    // Do not use cursor.
                    preparedStatement.setFetchSize(0);
                } else {
                    // Use a cursor, but fetch a lot of rows at once.
                    // Setting fetchSize to something other than zero enables server-side cursor use.
                    // This will only be effective if autoCommit=false though. Otherwise it fills up the memory with all rows.
                    // By default prepared statements are forward-only and read-only (though we could set that explicitly).
                    // Those settings allow cursors to be used efficiently.
                    preparedStatement.setFetchSize(1000);
                }
                if (id != null) {
                    // Fill the primary key into the prepared statement
                    preparedStatement.setString(1, id);
                }
                // Display the SQL statement for clarity
                LOG.info(preparedStatement.toString());
                results = preparedStatement.executeQuery();
                hasMoreEntities = results.next();
            } catch (SQLException sqlEx) {
                DbUtils.closeQuietly(connection);
                if (SQL_STATE_UNDEFINED_TABLE.equals(sqlEx.getSQLState())) {
                    // Table is just missing, iterate as if it were an empty table.
                    LOG.info("Table {} did not exist, returning an iterator as if it were empty.", qualifiedTableName);
                    results = null;
                    hasMoreEntities = false;
                }
            } catch (Exception ex) {
                DbUtils.closeQuietly(connection);
                throw new StorageException(ex);
            }
            // Note that we close the connection in the catch clauses above, not in a finally clause.
            // This is because we want to leave the connection open for the iterator to continue fetching results.
        }

        /**
         * Tell the caller whether any object will be returned by a call to next().
         */
        @Override
        public boolean hasNext () {
            return hasMoreEntities;
        }

        /**
         * If you iterate all the way through to the end of the iterator the connection will automatically be closed.
         * This allows concise (for Stop stop : feed.stops) iteration.
         * However it does not allow for partial iteration - stopping partway through will leave the connection open.
         */
        @Override
        public T next() {
            try {
                T entity = entityPopulator.populate(results, columnForName);
                entity.sourceFileLine = EntityPopulator.getIntIfPresent(results, "csv_line", columnForName);
                hasMoreEntities = results.next();
                if (!hasMoreEntities) {
                    // No more entities to iterate over. We can close the database connection.
                    // Closing the connection will also close all result sets and return the connection to the pool.
                    connection.close();
                }
                return entity;
            } catch (Exception ex) {
                DbUtils.closeQuietly(connection);
                throw new StorageException(ex);
            }
        }

        /**
         * The finalizer will be called when the object is garbage collected.
         * This way we can detect unclosed connections.
         */
        @Override
        public void finalize () {
            try {
                if (connection != null && !connection.isClosed()) {
                    LOG.error("An iterator connection to table {} is being closed in a finalizer, " +
                            "it should have been closed at the end of iteration.", qualifiedTableName);
                    connection.close();
                }
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }

    }


}
