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

/**
 * Field type for a list of date strings. This type is ONLY used for the editor-specific schedule exceptions table, so
 * not all methods are supported (e.g., validateAndConvert has no functionality because it is only called on GTFS tables).
 */
public class DateListField extends Field {

    public DateListField(String name, Requirement requirement) {
        super(name, requirement);
    }

    @Override
    public ValidateFieldResult<String> validateAndConvert(String original) {
        // This function is currently only used during feed loading. Because the DateListField only exists for the
        // schedule exceptions table (which is not a GTFS spec table), we do not expect to ever call this function on
        // this table/field.
        // TODO: is there any reason we may want to add support for validation?
        throw new UnsupportedOperationException("Cannot call validateAndConvert on field of type DateListField because this is not a supported GTFS field type.");
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
