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
 * Field type for a list of strings. This type is ONLY used for the storage of fare rules (i.e., the field type is not
 * found or expected in CSV tables, so not all methods are supported (e.g., validateAndConvert has no functionality
 * because it is only called on GTFS tables).
 */
public class StringListField extends Field {

    public StringListField(String name, Requirement requirement) {
        super(name, requirement);
    }

    @Override
    public ValidateFieldResult<String> validateAndConvert(String original) {
        // This function is currently only used during feed loading. Because the StringListField only exists for the
        // schedule exceptions table (which is not a GTFS spec table), we do not expect to ever call this function on
        // this table/field.
        // TODO: is there any reason we may want to add support for validation?
        throw new UnsupportedOperationException("Cannot call validateAndConvert on field of type StringListField because this is not a supported GTFS field type.");
    }

    @Override
    public Set<NewGTFSError> setParameter(PreparedStatement preparedStatement, int oneBasedIndex, String string) {
        try {
            // Only split on commas following an escaped quotation mark, as this indicates a new item in the list.
            String[] stringList = string.split("(?<=\"),");
            // Clean the string list of any escaped quotations which are required to preserve any internal commas.
            stringList = Arrays.stream(stringList).map(s -> s.replace("\"", "")).toArray(String[]::new);
            Array array = preparedStatement.getConnection().createArrayOf("text", stringList);
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
