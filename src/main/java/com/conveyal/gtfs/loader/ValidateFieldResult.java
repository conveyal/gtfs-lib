package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.error.NewGTFSError;

import java.util.HashSet;
import java.util.Set;

/**
 * This mini helper class is used during feed loading to return both:
 * - a cleaned value of arbitrary type T and
 * - any errors encountered while validating the original value.
 *
 * Previously we resorted to returning a single validated value and throwing exceptions if bad values were encountered,
 * but this kept us from being able to do both things at once: repair the value AND collect errors on the offending input.
 */
public class ValidateFieldResult<T> {
    public T clean;
    public Set<NewGTFSError> errors = new HashSet<>();

    public ValidateFieldResult() {}

    /** Constructor used to set a default value (which may then be updated with the clean value). */
    public ValidateFieldResult(T defaultValue) {
        this.clean = defaultValue;
    }

    /** Builder method that constructs a ValidateFieldResult with type String from the input result. */
    public static ValidateFieldResult<String> from(ValidateFieldResult result) {
        ValidateFieldResult<String> stringResult = new ValidateFieldResult<>();
        stringResult.clean = String.valueOf(result.clean);
        stringResult.errors.addAll(result.errors);
        return stringResult;
    }

}
