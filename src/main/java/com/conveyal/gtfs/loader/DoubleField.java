package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.storage.StorageException;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLType;
import java.util.Set;

import static com.conveyal.gtfs.error.NewGTFSErrorType.NUMBER_PARSING;
import static com.conveyal.gtfs.error.NewGTFSErrorType.NUMBER_TOO_LARGE;
import static com.conveyal.gtfs.error.NewGTFSErrorType.NUMBER_TOO_SMALL;

/**
 * Created by abyrd on 2017-03-31
 */
public class DoubleField extends Field {

    private double minValue;
    private double maxValue;
    // This field dictates how many decimal places this field should be rounded to when exporting to a GTFS.
    // The place where the rounding happens during exports is in Table.commaSeparatedNames.
    // A value less than 0 indicates that no rounding should happen.
    private int outputPrecision;

    public DoubleField (String name, Requirement requirement, double minValue, double maxValue, int outputPrecision) {
        super(name, requirement);
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.outputPrecision = outputPrecision;
    }

    private ValidateFieldResult<Double> validate(String string) {
        ValidateFieldResult<Double> result = new ValidateFieldResult<>();
        try {
            result.clean = Double.parseDouble(string);
        } catch (NumberFormatException e) {
            throw new StorageException(NUMBER_PARSING, string);
        }
        if (result.clean < minValue) NewGTFSError.forFeed(NUMBER_TOO_SMALL, string);
        if (result.clean > maxValue) NewGTFSError.forFeed(NUMBER_TOO_LARGE, string);
        return result;
    }

    @Override
    public Set<NewGTFSError> setParameter(PreparedStatement preparedStatement, int oneBasedIndex, String string) {
        try {
            ValidateFieldResult<Double> result = validate(string);
            preparedStatement.setDouble(oneBasedIndex, result.clean);
            return result.errors;
        } catch (Exception ex) {
            throw new StorageException(ex);
        }
    }

    @Override
    public ValidateFieldResult<String> validateAndConvert(String string) {
        return ValidateFieldResult.from(validate(string));
    }

    @Override
    public SQLType getSqlType () {
        return JDBCType.DOUBLE;
    }

    @Override
    public String getSqlTypeName () {
        return "double precision";
    }

    /**
     * When outputting to csv, round fields that have been created with an outputPrecision > -1 to avoid excessive
     * precision.
     */
    @Override
    public String getColumnExpression(String prefix, boolean csvOutput) {
        String columnName = super.getColumnExpression(prefix, csvOutput);
        if (!csvOutput || this.outputPrecision < 0) return columnName;
        return String.format(
            "round(%s::DECIMAL, %d) as %s",
            columnName,
            this.outputPrecision,
            name
        );
    }

}
