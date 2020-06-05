package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.storage.StorageException;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLType;
import java.util.Set;

public class IntegerField extends Field {

    private int minValue;

    private int maxValue;

    public IntegerField(String name, Requirement required) {
        this(name, required, 0, Integer.MAX_VALUE);
    }

    public IntegerField(String name, Requirement requirement, int maxValue) {
        this(name, requirement, 0, maxValue);
    }

    public IntegerField(String name, Requirement requirement, int minValue, int maxValue) {
        super(name, requirement);
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    private ValidateFieldResult<Integer> validate (String string) {
        ValidateFieldResult<Integer> result = new ValidateFieldResult<>();
        try {
            result.clean = Integer.parseInt(string);
        } catch (NumberFormatException e) {
            result.errors.add(NewGTFSError.forFeed(NewGTFSErrorType.NUMBER_PARSING, string));
        }
        if (result.clean < minValue) result.errors.add(NewGTFSError.forFeed(NewGTFSErrorType.NUMBER_TOO_SMALL, string));
        if (result.clean > maxValue) result.errors.add(NewGTFSError.forFeed(NewGTFSErrorType.NUMBER_TOO_LARGE, string));
        return result;
    }

    @Override
    public Set<NewGTFSError> setParameter (PreparedStatement preparedStatement, int oneBasedIndex, String string) {
        try {
            ValidateFieldResult<Integer> result = validate(string);
            preparedStatement.setInt(oneBasedIndex, result.clean);
            return result.errors;
        } catch (Exception ex) {
            throw new StorageException(ex);
        }
    }

    @Override
    public ValidateFieldResult<String> validateAndConvert (String string) {
        return ValidateFieldResult.from(validate(string));
    }

    @Override
    public SQLType getSqlType () {
        return JDBCType.INTEGER;
    }

}
