package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.model.Entity;
import com.conveyal.gtfs.storage.StorageException;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    PreparedStatement selectAll;
    PreparedStatement select;
    EntityPopulator<T> entityPopulator;
    TObjectIntMap<String> columnForName;

    /**
     * @param tablePrefix must not be null, can be empty string, should include any separator character
     */
    public JDBCTableReader(Table specTable, Connection connection, String tablePrefix, EntityPopulator<T> entityPopulator) {
        String qualifiedTableName = tablePrefix + specTable.name;
        this.entityPopulator = entityPopulator;
        try {
            String selectAllSql = "select * from " + qualifiedTableName;
            selectAll = connection.prepareStatement(selectAllSql, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, CLOSE_CURSORS_AT_COMMIT);
            // Setting fetchSize to something other than zero enables server-side cursor use.
            // This will only be effective if autoCommit=false though. Otherwise it fills up the memory with all rows.
            // By default prepared statements are forward-only and read-only, but it wouldn't hurt to show that explicitly.
            // Those settings allow cursors to be used efficiently.
            selectAll.setFetchSize(1000); // Use a cursor, but fetch a lot of rows at once.
            // Cache the index for each column name to avoid throwing exceptions for missing columns.
            ResultSetMetaData metaData = selectAll.getMetaData();
            int nColumns = metaData.getColumnCount();
            // No entry value defaults to zero, and SQL columns are 1-based.
            columnForName = new TObjectIntHashMap<>(nColumns);
            for (int c = 1; c <= nColumns; c++) columnForName.put(metaData.getColumnName(c), c);
            String idField = specTable.getKeyFieldName();
            String selectOneSql = String.format("select * from %s where %s = ?", qualifiedTableName, idField);
            String orderByField = specTable.getOrderFieldName();
            if (orderByField != null) selectOneSql += " order by " + orderByField;
            select = connection.prepareStatement(selectOneSql, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, CLOSE_CURSORS_AT_COMMIT);
            select.setFetchSize(0); // Do not use cursor
            // Redefine select all to be ordered TODO pull this into a separate statement
            if (orderByField != null) {
                selectAllSql = String.format("select * from %s order by %s, %s", qualifiedTableName, idField, orderByField);
                selectAll = connection.prepareStatement(selectAllSql, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, CLOSE_CURSORS_AT_COMMIT);
                selectAll.setFetchSize(1000);
            }
        } catch (SQLException ex) {
            throw new StorageException(ex);
        }

    }

    @Override
    public Iterator<T> iterator() {
        return new EntityIterator(selectAll); //
    }

    /**
     * Get a single item from this table by ID.
     */
    public T get (String id) {
        try {
            select.setString(1, id);
            ResultSet result = select.executeQuery();
            if (result.next()) {
                return entityPopulator.populate(result, columnForName);
            } else {
                return null;
            }
        } catch (SQLException ex) {
            throw new StorageException(ex);
        }
    }

    /**
     * Get all the items from this table with the given ID, in order.
     */
    public Iterable<T> getOrdered (final String id) {
        // An iterable is a single function that produces an iterator.
        return () -> {
            try {
                // Set the prepared statement parameter immediately before the iterator is constructed.
                select.setString(1, id);
                return new EntityIterator(select);
            } catch (Exception ex) {
                throw new StorageException(ex);
            }
        };
    }

    private class EntityIterator implements Iterator<T> {

        boolean hasMoreEntities;
        ResultSet results;

        EntityIterator (PreparedStatement preparedStatement) {
            try {
                // LOG.info(preparedStatement.toString()); // show SQL
                results = preparedStatement.executeQuery();
                hasMoreEntities = results.next();
            } catch (Exception ex) {
                throw new StorageException(ex);
            }
        }

        @Override
        public boolean hasNext () {
            return hasMoreEntities;
        }

        @Override
        public T next() {
            try {
                T entity = entityPopulator.populate(results, columnForName);
                hasMoreEntities = results.next();
                if (!hasMoreEntities) {
                    results.close();
                    // This blocks nested iteration over different tables.
                    //selectAll.getConnection().commit();
                }
                return entity;
            } catch (Exception ex) {
                throw new StorageException(ex);
            }
        }

    }


}
