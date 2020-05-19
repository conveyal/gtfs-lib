package com.conveyal.gtfs.storage;

/**
 * A helper class to verify that data got stored in a particular record.
 */
public class RecordExpectation {
    public double acceptedDelta;
    public double doubleExpectation;
    public String editorExpectation;
    public ExpectedFieldType expectedFieldType;
    public String fieldName;
    public int intExpectation;
    public String stringExpectation;
    public boolean stringExpectationInCSV = false;
    public boolean editorStringExpectation = false;

    public RecordExpectation(String fieldName, int intExpectation) {
        this.fieldName = fieldName;
        this.expectedFieldType = ExpectedFieldType.INT;
        this.intExpectation = intExpectation;
    }

    public static RecordExpectation[] list(RecordExpectation... expectations) {
        return expectations;
    }

    /**
     * This extra constructor is a bit hacky in that it is only used for certain records that have
     * an int type when stored in the database, but a string type when exported to GTFS
     */
    public RecordExpectation(String fieldName, int intExpectation, String stringExpectation) {
        this.fieldName = fieldName;
        this.expectedFieldType = ExpectedFieldType.INT;
        this.intExpectation = intExpectation;
        this.stringExpectation = stringExpectation;
        this.stringExpectationInCSV = true;
    }

    /**
     * This extra constructor is a also hacky in that it is only used for records that have
     * an int type when stored in the database, and different values in the CSV export depending on
     * whether or not it is a snapshot from the editor.  Currently this only applies to stop_times.stop_sequence
     */
    public RecordExpectation(String fieldName, int intExpectation, String stringExpectation, String editorExpectation) {
        this.fieldName = fieldName;
        this.expectedFieldType = ExpectedFieldType.INT;
        this.intExpectation = intExpectation;
        this.stringExpectation = stringExpectation;
        this.stringExpectationInCSV = true;
        this.editorStringExpectation = true;
        this.editorExpectation = editorExpectation;
    }

    public RecordExpectation(String fieldName, String stringExpectation) {
        this.fieldName = fieldName;
        this.expectedFieldType = ExpectedFieldType.STRING;
        this.stringExpectation = stringExpectation;
    }

    public RecordExpectation(String fieldName, double doubleExpectation, double acceptedDelta) {
        this.fieldName = fieldName;
        this.expectedFieldType = ExpectedFieldType.DOUBLE;
        this.doubleExpectation = doubleExpectation;
        this.acceptedDelta = acceptedDelta;
    }

    /** Constructor only used for {@link #clone}. */
    private RecordExpectation(String fieldname) {
        this.fieldName = fieldname;
    }

    public String getStringifiedExpectation(boolean fromEditor) {
        if (fromEditor && editorStringExpectation) return editorExpectation;
        if (stringExpectationInCSV) return stringExpectation;
        switch (expectedFieldType) {
            case DOUBLE:
                return String.valueOf(doubleExpectation);
            case INT:
                return String.valueOf(intExpectation);
            case STRING:
                return stringExpectation;
            default:
                return null;
        }
    }

    /** Clone a record expectation. Note: new fields should be added here. */
    public RecordExpectation clone() {
        RecordExpectation copy = new RecordExpectation(this.fieldName);
        copy.expectedFieldType = this.expectedFieldType;
        copy.editorExpectation = this.editorExpectation;
        copy.intExpectation = this.intExpectation;
        copy.stringExpectation = this.stringExpectation;
        copy.stringExpectationInCSV = this.stringExpectationInCSV;
        copy.acceptedDelta = this.acceptedDelta;
        copy.doubleExpectation = this.doubleExpectation;
        return copy;
    }

    @Override public String toString() {
        return "RecordExpectation{" + "acceptedDelta=" + acceptedDelta + ", doubleExpectation=" + doubleExpectation
            + ", editorExpectation='" + editorExpectation + '\'' + ", expectedFieldType=" + expectedFieldType
            + ", fieldName='" + fieldName + '\'' + ", intExpectation=" + intExpectation + ", stringExpectation='"
            + stringExpectation + '\'' + ", stringExpectationInCSV=" + stringExpectationInCSV
            + ", editorStringExpectation=" + editorStringExpectation + '}';
    }
}
