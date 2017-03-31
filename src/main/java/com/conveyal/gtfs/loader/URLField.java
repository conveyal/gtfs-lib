package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.storage.StorageException;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.PreparedStatement;

/**
 * Created by abyrd on 2017-03-31
 */
public class URLField extends Field {

    public URLField(String name, Requirement requirement) {
        super(name, requirement);
    }

    /** Check that a string can be properly parsed and is in range. */
    public String validateAndConvert(String original) {
        try {
            String cleanString = cleanString(original);
            // new URL(cleanString); TODO call this to validate, but we can't default to zero
            return cleanString;
        } catch (Exception ex) {
            throw new StorageException(ex);
        }
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
