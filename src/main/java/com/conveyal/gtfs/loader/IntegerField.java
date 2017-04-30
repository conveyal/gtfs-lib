package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.storage.StorageException;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLType;

/**
 * Created by abyrd on 2017-03-31
 */
public class IntegerField extends Field {

    private int maxValue;

    public IntegerField(String name, Requirement required) {
        this(name, required, Integer.MAX_VALUE);
    }

    public IntegerField(String name, Requirement requirement, int maxValue) {
        super(name, requirement);
        this.maxValue = maxValue;
    }

    private int validate (String string) {
        int i = Integer.parseInt(string);
        if (i < 0) throw new StorageException("negative field");
        if (i > maxValue) throw new StorageException("excessively large integer value");
        return i;
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
        validate(string);
        return string;
    }

    @Override
    public SQLType getSqlType () {
        return JDBCType.INTEGER;
    }

}
