package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.storage.StorageException;

import java.sql.Array;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLType;

public class StringListField extends Field {

    public StringListField(String name, Requirement requirement) {
        super(name, requirement);
    }

    @Override
    public String validateAndConvert(String original) {
        // FIXME
        return null;
    }

    @Override
    public void setParameter(PreparedStatement preparedStatement, int oneBasedIndex, String string) {
        // FIXME
        try {
            Array array = preparedStatement.getConnection().createArrayOf("text", string.split(","));
            preparedStatement.setArray(oneBasedIndex, array);
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
