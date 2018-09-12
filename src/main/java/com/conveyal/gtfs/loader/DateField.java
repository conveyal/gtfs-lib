package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.storage.StorageException;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLType;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * A GTFS date in the format YYYYMMDD.
 * While Postgres has a date type, SQLite does not have any date types, let alone a specific timezone free one.
 * Therefore we just save this as a string. Alternatively we could save an integer, but then we'd lose some detail
 * about malformed strings.
 */
public class DateField extends Field {

    // DateTimeFormatter.BASIC_ISO_DATE is also yyyyMMdd with no separator,
    // but allows timezones which are not part of GTFS dates.
    public static final DateTimeFormatter GTFS_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    public DateField (String name, Requirement requirement) {
        super(name, requirement);
    }

    public static String validate (String string) {
        // Parse the date out of the supplied string.
        LocalDate date;
        try {
            date = LocalDate.parse(string, GTFS_DATE_FORMATTER);
        } catch (DateTimeParseException ex) {
            throw new StorageException(NewGTFSErrorType.DATE_FORMAT, string);
        }
        // Range check on year. Parsing operation above should already have checked month and day ranges.
        int year = date.getYear();
        if (year < 2000 || year > 2100) {
            throw new StorageException(NewGTFSErrorType.DATE_RANGE, string);
        }
        return string;
    }

    @Override
    public void setParameter (PreparedStatement preparedStatement, int oneBasedIndex, String string) {
        try {
            preparedStatement.setString(oneBasedIndex, validate(string));
        } catch (Exception ex) {
            throw new StorageException(ex);
        }
    }

    /**
     * DateField specific method to set a statement parameter from a {@link LocalDate}.
     */
    public void setParameter (PreparedStatement preparedStatement, int oneBasedIndex, LocalDate localDate) {
        try {
            if (localDate == null) setNull(preparedStatement, oneBasedIndex);
            else preparedStatement.setString(oneBasedIndex, localDate.format(GTFS_DATE_FORMATTER));
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public String validateAndConvert (String string) {
        return validate(string);
    }

    @Override
    public SQLType getSqlType () {
        return JDBCType.VARCHAR;
    }

}
