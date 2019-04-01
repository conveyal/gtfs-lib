package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.model.Entity;

import java.io.IOException;
import java.sql.SQLException;

/**
 * This is an interface for classes to create, update, or delete a GTFS entity given an Integer ID and/or JSON string.
 * Created by landon on 2017-12-20
 */
public interface TableWriter <T extends Entity> {

    // FIXME: add optional auto-commit boolean so that additional changes can be made?
    String create (String json, boolean autoCommit) throws SQLException, IOException;

    String update (Integer id, String json, boolean autoCommit) throws SQLException, IOException;

    int delete (Integer id, boolean autoCommit) throws SQLException;

    int deleteWhere (String fieldName, String value, boolean autoCommit) throws SQLException;

    void commit () throws SQLException;

    void close ();
}
