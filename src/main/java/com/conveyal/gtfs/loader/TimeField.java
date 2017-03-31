package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.storage.StorageException;
import com.j256.ormlite.stmt.query.In;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * A field in the format HH:MM:SS, which will be stored as a number of seconds after midnight.
 */
public class TimeField extends Field {

    public TimeField(String name, Requirement requirement) {
        super(name, requirement);
    }

    @Override
    public void setPreparedStatementParameter (int oneBasedIndex, String string, PreparedStatement preparedStatement) {
        try {
            preparedStatement.setInt(oneBasedIndex, getSeconds(string));
        } catch (Exception ex) {
            throw new StorageException(ex);
        }
    }

    // Actually this is converting the string. Can we use some JDBC existing functions for this?
    @Override
    public String validateAndConvert(String hhmmss) {
        return Integer.toString(getSeconds(cleanString(hhmmss)));
    }

    private static int getSeconds (String hhmmss) {
        String[] fields = hhmmss.split(":");
        int h = Integer.parseInt(fields[0]);
        int m = Integer.parseInt(fields[1]);
        int s = Integer.parseInt(fields[2]);
        return ((h * 60) + m) * 60 + s;
    }

    @Override
    public String getSqlType () {
        return "integer";
    }

}
