package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.storage.StorageException;

import java.sql.PreparedStatement;

/**
 * Created by abyrd on 2017-03-31
 */
public class StringField extends Field {

    public StringField (String name, Requirement requirement) {
        super(name, requirement);
    }

    /** Check that a string can be properly parsed and is in range. */
    public String validateAndConvert(String original) {
        return cleanString(original);
    }

    public void setPreparedStatementParameter (int oneBasedIndex, String string, PreparedStatement preparedStatement) {
        try {
            preparedStatement.setString(oneBasedIndex, validateAndConvert(string));
        } catch (Exception ex) {
            throw new StorageException(ex);
        }
    }

    @Override
    public String getSqlType() {
        return "varchar";
    }

}
