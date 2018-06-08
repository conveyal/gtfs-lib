package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.storage.StorageException;

import java.sql.Array;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLType;
import java.util.Arrays;
import java.util.stream.Collectors;

import static com.conveyal.gtfs.loader.DateField.validate;

public class DateListField extends Field {

    public DateListField(String name, Requirement requirement) {
        super(name, requirement);
    }

    @Override
    public String validateAndConvert(String original) {
        // FIXME
        return null;
    }

    @Override
    public void setParameter(PreparedStatement preparedStatement, int oneBasedIndex, String string) {
        try {
            String[] dateStrings = Arrays.stream(string.split(","))
                    .map(DateField::validate)
                    .toArray(String[]::new);
            Array array = preparedStatement.getConnection().createArrayOf("text", dateStrings);
            preparedStatement.setArray(oneBasedIndex, array);
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
