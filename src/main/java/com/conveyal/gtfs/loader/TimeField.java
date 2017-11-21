package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.storage.StorageException;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLType;

/**
 * A field in the format HH:MM:SS, which will be stored as a number of seconds after midnight.
 */
public class TimeField extends Field {

    public TimeField(String name, Requirement requirement) {
        super(name, requirement);
    }

    @Override
    public void setParameter(PreparedStatement preparedStatement, int oneBasedIndex, String string) {
        try {
            preparedStatement.setInt(oneBasedIndex, getSeconds(string));
        } catch (Exception ex) {
            throw new StorageException(ex);
        }
    }

    // Actually this is converting the string. Can we use some JDBC existing functions for this?
    @Override
    public String validateAndConvert(String hhmmss) {
        return Integer.toString(getSeconds(hhmmss));
    }

    private static int getSeconds (String hhmmss) {
        // Accept hh:mm:ss or h:mm:ss for single-digit hours.
        if (hhmmss.length() != 8 && hhmmss.length() != 7) {
            throw new StorageException(NewGTFSErrorType.TIME_FORMAT, hhmmss);
        }
        String[] fields = hhmmss.split(":");
        if (fields.length != 3) {
            throw new StorageException(NewGTFSErrorType.TIME_FORMAT, hhmmss);
        }
        int h = Integer.parseInt(fields[0]);
        int m = Integer.parseInt(fields[1]);
        int s = Integer.parseInt(fields[2]);
        // Other than the Moscow-Pyongyang route at 8.5 days, most of the longest services are around 6 days.
        if (h < 0) throw new StorageException(NewGTFSErrorType.NUMBER_NEGATIVE, hhmmss);
        if (h > 150) throw new StorageException(NewGTFSErrorType.NUMBER_TOO_LARGE, hhmmss);
        if (m < 0) throw new StorageException(NewGTFSErrorType.NUMBER_NEGATIVE, hhmmss);
        if (m > 59) throw new StorageException(NewGTFSErrorType.NUMBER_TOO_LARGE, hhmmss);
        if (s < 0) throw new StorageException(NewGTFSErrorType.NUMBER_NEGATIVE, hhmmss);
        if (s > 59) throw new StorageException(NewGTFSErrorType.NUMBER_TOO_LARGE, hhmmss);
        return ((h * 60) + m) * 60 + s;
    }

    @Override
    public SQLType getSqlType () {
        return JDBCType.INTEGER;
    }

}
