package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.storage.StorageException;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLType;
import java.util.Collections;
import java.util.Set;

/**
 * A field in the format HH:MM:SS, which will be stored as a number of seconds after midnight.
 */
public class TimeField extends Field {

    public TimeField(String name, Requirement requirement) {
        super(name, requirement);
    }

    @Override
    public Set<NewGTFSError> setParameter(PreparedStatement preparedStatement, int oneBasedIndex, String string) {
        try {
            ValidateFieldResult<Integer> result = getSeconds(string);
            preparedStatement.setInt(oneBasedIndex, result.clean);
            return result.errors;
        } catch (Exception ex) {
            throw new StorageException(ex);
        }
    }

    // Actually this is converting the string. Can we use some JDBC existing functions for this?
    @Override
    public ValidateFieldResult<String> validateAndConvert(String hhmmss) {
        return ValidateFieldResult.from(getSeconds(hhmmss));
    }

    private static ValidateFieldResult<Integer> getSeconds (String hhmmss) {
        ValidateFieldResult<Integer> result = new ValidateFieldResult<>();
        // Accept hh:mm:ss or h:mm:ss for single-digit hours.
        if (hhmmss.length() != 8 && hhmmss.length() != 7) {
            result.errors.add(NewGTFSError.forFeed(NewGTFSErrorType.TIME_FORMAT, hhmmss));
            return result;
        }
        String[] fields = hhmmss.split(":");
        if (fields.length != 3) {
            result.errors.add(NewGTFSError.forFeed(NewGTFSErrorType.TIME_FORMAT, hhmmss));
            return result;
        }
        int h = Integer.parseInt(fields[0]);
        int m = Integer.parseInt(fields[1]);
        int s = Integer.parseInt(fields[2]);
        // Other than the Moscow-Pyongyang route at 8.5 days, most of the longest services are around 6 days.
        if (h < 0) result.errors.add(NewGTFSError.forFeed(NewGTFSErrorType.NUMBER_NEGATIVE, hhmmss));
        if (h > 150) result.errors.add(NewGTFSError.forFeed(NewGTFSErrorType.NUMBER_TOO_LARGE, hhmmss));
        if (m < 0) result.errors.add(NewGTFSError.forFeed(NewGTFSErrorType.NUMBER_NEGATIVE, hhmmss));
        if (m > 59) result.errors.add(NewGTFSError.forFeed(NewGTFSErrorType.NUMBER_TOO_LARGE, hhmmss));
        if (s < 0) result.errors.add(NewGTFSError.forFeed(NewGTFSErrorType.NUMBER_NEGATIVE, hhmmss));
        if (s > 59) result.errors.add(NewGTFSError.forFeed(NewGTFSErrorType.NUMBER_TOO_LARGE, hhmmss));
        result.clean = ((h * 60) + m) * 60 + s;
        return result;
    }

    @Override
    public SQLType getSqlType () {
        return JDBCType.INTEGER;
    }

    /**
     * When outputting to csv, return the PostgreSQL syntax to convert seconds since midnight into the time format
     * HH:MM:SS for the specified field.
     */
    @Override
    public String getColumnExpression(String prefix, boolean csvOutput) {
        String columnName = super.getColumnExpression(prefix, csvOutput);
        return csvOutput
            ? String.format("TO_CHAR((%s || ' second')::interval, 'HH24:MI:SS') as %s", columnName, name)
            : columnName;
    }

}
