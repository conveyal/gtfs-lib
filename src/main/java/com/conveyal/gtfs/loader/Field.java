package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.storage.StorageException;

import java.sql.PreparedStatement;
import java.sql.SQLType;

/**
 * Created by abyrd on 2017-03-30
 */
public abstract class Field {

    final String name;
    final Requirement requirement;

    public Field(String name, Requirement requirement) {
        this.name = name;
        this.requirement = requirement;
    }

    /**
     * Check the supplied string to see if it can be parsed as the proper data type.
     * Perform any conversion (I think this is only done for times, to integer numbers of seconds).
     * @param original a non-null String
     * @return a string that is parseable as this field's type, or null if it is not parseable
     */
    public abstract String validateAndConvert(String original);

    public abstract void setParameter(PreparedStatement preparedStatement, int oneBasedIndex, String string);

    public abstract SQLType getSqlType ();

    // Overridden to create exception for "double precision", since its enum value is just called DOUBLE.
    public String getSqlTypeName () {
        return getSqlType().getName();
    }

    public String getSqlDeclaration() {
        return String.join(" ", name, getSqlTypeName());
    }

    // TODO test for input with tabs, newlines, carriage returns, and slashes in it.
    protected static String cleanString (String string) {
        // Backslashes, newlines, and tabs have special meaning to Postgres.
        // String.contains is significantly faster than using a regex or replace, and has barely any speed impact.
        if (string.contains("\\")) {
            string = string.replace("\\", "\\\\");
        }
        if (string.contains("\t") || string.contains("\n") || string.contains("\r")) {
            // TODO record error and recover, and use a single regex
            string = string.replace("\t", "");
            string = string.replace("\n", "");
            string = string.replace("\r", "");
        }
        return string;
    }

    /**
     * Generally any required field should be present on every row.
     * TODO override this method for exceptions, e.g. arrival and departure can be missing though the field must be present
     */
    public boolean missingRequired (String string) {
        return  (string == null || string.isEmpty()) && this.isRequired();
    }

    public boolean isRequired () {
        return this.requirement == Requirement.REQUIRED;
    }

}
