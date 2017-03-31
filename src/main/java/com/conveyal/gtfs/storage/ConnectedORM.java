package com.conveyal.gtfs.storage;

import java.sql.Connection;

/**
 * Created by abyrd on 2017-03-27
 */
public class ConnectedORM {

    Connection connection;
    ObjectRelationalMapping orm;

    public ConnectedORM (Connection connection, ObjectRelationalMapping orm) {
        this.connection = connection;
        this.orm = orm;
    }

//    public void store (T entity) {
//
//    }
//

}
