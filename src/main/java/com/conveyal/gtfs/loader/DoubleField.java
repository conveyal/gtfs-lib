package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.storage.StorageException;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLType;

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

    private double validate(String string) {
        double d;
        try {
            d = Double.parseDouble(string);
        } catch (NumberFormatException e) {
            throw new StorageException(NUMBER_PARSING, string);
        }
        if (d < minValue) throw new StorageException(NUMBER_TOO_SMALL, Double.toString(d));
        if (d > maxValue) throw new StorageException(NUMBER_TOO_LARGE, Double.toString(d));
        return d;
    }

    @Override
    public void setParameter(PreparedStatement preparedStatement, int oneBasedIndex, String string) {
        try {
            preparedStatement.setDouble(oneBasedIndex, validate(string));
        } catch (Exception ex) {
            throw new StorageException(ex);
        }
    }

    @Override
    public String validateAndConvert(String string) {
        validate(string);
        return string;
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
