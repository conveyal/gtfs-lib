package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.storage.StorageException;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLType;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.Set;

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

    public static ValidateFieldResult<String> validate (String string) {
        // Initialize default value as null (i.e., don't use the input value).
        ValidateFieldResult<String> result = new ValidateFieldResult<>();
        // Parse the date out of the supplied string.
        LocalDate date;
        try {
            date = LocalDate.parse(string, GTFS_DATE_FORMATTER);
            // Only set the clean result after the date parse is successful.
            result.clean = string;
        } catch (DateTimeParseException ex) {
            result.errors.add(NewGTFSError.forFeed(NewGTFSErrorType.DATE_FORMAT, string));
            return result;
        }
        // Range check on year. Parsing operation above should already have checked month and day ranges.
        int year = date.getYear();
        if (year < 2000 || year > 2100) {
            result.errors.add(NewGTFSError.forFeed(NewGTFSErrorType.DATE_RANGE, string));
        }
        return result;
    }

    @Override
    public Set<NewGTFSError> setParameter (PreparedStatement preparedStatement, int oneBasedIndex, String string) {
        try {
            ValidateFieldResult<String> result = validate(string);
            preparedStatement.setString(oneBasedIndex, result.clean);
            return result.errors;
        } catch (Exception ex) {
            throw new StorageException(ex);
        }
    }

    /**
     * DateField specific method to set a statement parameter from a {@link LocalDate}.
     */
    public Set<NewGTFSError> setParameter (PreparedStatement preparedStatement, int oneBasedIndex, LocalDate localDate) {
        try {
            if (localDate == null) setNull(preparedStatement, oneBasedIndex);
            else preparedStatement.setString(oneBasedIndex, localDate.format(GTFS_DATE_FORMATTER));
            return Collections.EMPTY_SET;
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public ValidateFieldResult<String> validateAndConvert (String string) {
        return validate(string);
    }

    @Override
    public SQLType getSqlType () {
        return JDBCType.VARCHAR;
    }

}
