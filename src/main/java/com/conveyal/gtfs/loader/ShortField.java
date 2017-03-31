package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.storage.StorageException;

import java.sql.PreparedStatement;

/**
 * Created by abyrd on 2017-03-31
 */
public class ShortField extends Field {

    private int maxValue; // can be shared with all numeric field types?

    public ShortField (String name, Requirement requirement, int maxValue) {
        super(name, requirement);
        this.maxValue = maxValue;
    }

    private short validate (String string) {
        short s = Short.parseShort(string);
        if (s < 0) throw new StorageException("negative field in " + name  );
        if (s > maxValue) throw new StorageException("excessively large short integer value in field " + name);
        return s;
    }

    @Override
    public void setPreparedStatementParameter (int oneBasedIndex, String string, PreparedStatement preparedStatement) {
        try {
            preparedStatement.setShort(oneBasedIndex, validate(string));
        } catch (Exception ex) {
            throw new StorageException(ex);
        }
    }

    @Override
    public String validateAndConvert(String string) {
        validate(string);
        return string;
    }

    @Override
    public String getSqlType () {
        return "smallint";
    }

}
