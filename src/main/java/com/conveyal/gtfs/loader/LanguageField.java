package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.storage.StorageException;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLType;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

import static com.conveyal.gtfs.error.NewGTFSErrorType.LANGUAGE_FORMAT;

/**
 * Checks a BCP47 language tag.
 */
public class LanguageField extends Field {

    public LanguageField (String name, Requirement requirement) {
        super(name, requirement);
    }

    private ValidateFieldResult<String> validate (String string) {
        ValidateFieldResult<String> result = new ValidateFieldResult<>(string);
        Locale locale = Locale.forLanguageTag(string);
        String generatedTag = locale.toLanguageTag();
        // This works except for hierarchical sublanguages like zh-cmn and zh-yue which get flattened to the sublanguage.
        if (!generatedTag.equalsIgnoreCase(string)) {
            result.errors.add(NewGTFSError.forFeed(LANGUAGE_FORMAT, string));
        }
        return result;
    }

    /** Check that a string can be properly parsed and is in range. */
    public ValidateFieldResult<String> validateAndConvert (String string) {
        return cleanString(validate(string));
    }

    public Set<NewGTFSError> setParameter(PreparedStatement preparedStatement, int oneBasedIndex, String string) {
        try {
            ValidateFieldResult<String> result = validateAndConvert(string);
            preparedStatement.setString(oneBasedIndex, result.clean);
            return result.errors;
        } catch (Exception ex) {
            throw new StorageException(LANGUAGE_FORMAT, string);
        }
    }

    @Override
    public SQLType getSqlType() {
        return JDBCType.VARCHAR;
    }

}
