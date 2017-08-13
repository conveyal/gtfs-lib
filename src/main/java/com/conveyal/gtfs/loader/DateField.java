package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.storage.StorageException;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLType;

/**
 * A GTFS date in the numeric format YYYYMMDD
 */
public class DateField extends Field {

    public DateField (String name, Requirement requirement) {
        super(name, requirement);
    }

    private int validate (String string) {
        if (string.length() != 8) {
            throw new StorageException("Date field should be exactly 8 characters long.");
        }
        int year = Integer.parseInt(string.substring(0, 4));
        int month = Integer.parseInt(string.substring(4, 6));
        int day = Integer.parseInt(string.substring(6, 8));
        if (year < 2000 || year > 2100) {
            throw new StorageException("Date year out of range 2000-2100: " + year);
        }
        if (month < 1 || month > 12) {
            throw new StorageException("Date month out of range 1-12: " + month);
        }
        if (day < 1 || day > 31) {
            throw new StorageException("Date day out of range 1-31: " + day);
        }
        return Integer.parseInt(string);
    }

    @Override
    public void setParameter (PreparedStatement preparedStatement, int oneBasedIndex, String string) {
        try {
            preparedStatement.setInt(oneBasedIndex, validate(string));
        } catch (Exception ex) {
            throw new StorageException(ex);
        }
    }

    @Override
    public String validateAndConvert (String string) {
        return Integer.toString(validate(string));
    }

    @Override
    public SQLType getSqlType () {
        return JDBCType.INTEGER;
    }

}
