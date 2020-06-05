package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.storage.StorageException;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLType;
import java.util.Set;

/**
 * Created by abyrd on 2017-03-31
 */
public class ShortField extends Field {

    private int maxValue; // can be shared with all numeric field types?

    public ShortField (String name, Requirement requirement, int maxValue) {
        super(name, requirement);
        this.maxValue = maxValue;
    }

    private ValidateFieldResult<Short> validate (String string) {
        ValidateFieldResult<Short> result = new ValidateFieldResult<>();
        if (string == null || string.isEmpty()) {
            // Default numeric fields to zero.
            result.clean = 0;
            return result;
        }
        result.clean = Short.parseShort(string);
        if (result.clean < 0) result.errors.add(NewGTFSError.forFeed(NewGTFSErrorType.NUMBER_NEGATIVE, string));
        if (result.clean > maxValue) result.errors.add(NewGTFSError.forFeed(NewGTFSErrorType.NUMBER_TOO_LARGE, string));
        return result;
    }

    @Override
    public Set<NewGTFSError> setParameter(PreparedStatement preparedStatement, int oneBasedIndex, String string) {
        try {
            ValidateFieldResult<Short> result = validate(string);
            preparedStatement.setShort(oneBasedIndex, result.clean);
            return result.errors;
        } catch (Exception ex) {
            throw new StorageException(ex);
        }
    }

    @Override
    public ValidateFieldResult<String> validateAndConvert(String string) {
        ValidateFieldResult<String> result = ValidateFieldResult.from(validate(string));
        return result;
    }

    @Override
    public SQLType getSqlType () {
        return JDBCType.SMALLINT;
    }

}
