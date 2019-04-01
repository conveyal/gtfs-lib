package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.storage.StorageException;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLType;
import java.util.Set;

/**
 * A GTFS boolean field, coded as a single character string 0 or 1.
 * It is stored in an SQL integer field.
 */
public class BooleanField extends Field {

    public BooleanField (String name, Requirement requirement) {
        super(name, requirement);
    }

    private ValidateFieldResult<Boolean> validate (String string) {
        ValidateFieldResult<Boolean> result = new ValidateFieldResult<>();
        if ( ! ("0".equals(string) || "1".equals(string))) {
            result.errors.add(NewGTFSError.forFeed(NewGTFSErrorType.BOOLEAN_FORMAT, string));
        }
        result.clean = "1".equals(string);
        return result;
    }

    @Override
    public Set<NewGTFSError> setParameter (PreparedStatement preparedStatement, int oneBasedIndex, String string) {
        try {
            ValidateFieldResult<Boolean> result = validate(string);
            preparedStatement.setBoolean(oneBasedIndex, result.clean);
            return result.errors;
        } catch (Exception ex) {
            throw new StorageException(ex);
        }
    }

    /**
     * The 0 or 1 will be converted to the string "true" or "false" for SQL COPY.
     */
    @Override
    public ValidateFieldResult<String> validateAndConvert (String string) {
        return ValidateFieldResult.from(validate(string));
    }

    @Override
    public SQLType getSqlType () {
        return JDBCType.BOOLEAN;
    }

}
