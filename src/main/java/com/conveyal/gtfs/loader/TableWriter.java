package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.model.Entity;

import java.sql.SQLException;

/**
 * This is an interface for classes to create, update, or delete a GTFS entity given an Integer ID and/or JSON string.
 * Created by landon on 2017-12-20
 */
public interface TableWriter <T extends Entity> {

    String create (String json) throws SQLException;

    String update (Integer id, String json) throws SQLException;

    int delete (Integer id) throws SQLException;
}
