package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.storage.StorageException;

import java.sql.Array;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLType;
import java.util.Collections;
import java.util.Set;

public class StringListField extends Field {

    public StringListField(String name, Requirement requirement) {
        super(name, requirement);
    }

    @Override
    public ValidateFieldResult<String> validateAndConvert(String original) {
        // FIXME: This function is currently only used during feed loading. Because the StringListField only exists for the
        //  schedule exceptions table (which is not a GTFS spec table), we do not expect to ever call this function on
        //  this table/field.
        return null;
    }

    @Override
    public Set<NewGTFSError> setParameter(PreparedStatement preparedStatement, int oneBasedIndex, String string) {
        // FIXME
        try {
            Array array = preparedStatement.getConnection().createArrayOf("text", string.split(","));
            preparedStatement.setArray(oneBasedIndex, array);
            return Collections.EMPTY_SET;
        } catch (Exception e) {
            throw new StorageException(e);
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
