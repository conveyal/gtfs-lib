package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.storage.StorageException;

import java.sql.Array;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLType;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

public class DateListField extends Field {

    public DateListField(String name, Requirement requirement) {
        super(name, requirement);
    }

    @Override
    public ValidateFieldResult<String> validateAndConvert(String original) {
        // FIXME: This function is currently only used during feed loading. Because the DateListField only exists for the
        //  schedule exceptions table (which is not a GTFS spec table), we do not expect to ever call this function on
        //  this table/field.
        return null;
    }

    @Override
    public Set<NewGTFSError> setParameter(PreparedStatement preparedStatement, int oneBasedIndex, String string) {
        try {
            String[] dateStrings = Arrays.stream(string.split(","))
                    .map(DateField::validate)
                    .toArray(String[]::new);
            Array array = preparedStatement.getConnection().createArrayOf("text", dateStrings);
            preparedStatement.setArray(oneBasedIndex, array);
            return Collections.EMPTY_SET;
        } catch (Exception ex) {
            throw new StorageException(ex);
        }
    }

    @Override
    public SQLType getSqlType() {
        return JDBCType.ARRAY;
    }

    @Override
    public String getSqlTypeName() {
        return "text[]";
    }
}
