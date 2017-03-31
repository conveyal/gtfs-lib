package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.storage.StorageException;

import java.sql.PreparedStatement;
import java.util.regex.Pattern;

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

    public abstract String validateAndConvert(String original);

    public abstract void setPreparedStatementParameter (int oneBasedIndex, String string, PreparedStatement preparedStatement);

    public abstract String getSqlType ();

    public String getSqlDeclaration() {
        return String.join(" ", name, getSqlType());
    }

    protected static String cleanString (String original) {
        // Backslashes, newlines, and tabs have special meaning to Postgres.
        // String.contains is significantly faster than using a regex or replace, and has barely any speed impact.
        if (original.contains("\\")) {
            return original.replace("\\", "\\\\");
        }
        if (original.contains("\t") || original.contains("\n") || original.contains("\r")) {
            throw new StorageException("Encountered tabs or newlines in field.");
        }
        return original;
    }


}
