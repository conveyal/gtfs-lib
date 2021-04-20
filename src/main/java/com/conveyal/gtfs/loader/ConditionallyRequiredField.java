package com.conveyal.gtfs.loader;

/**
 * These are the values that are checked inline with {@link ConditionallyRequiredFieldCheck} to determine if the required
 * conditions have been met.
 */
public class ConditionallyRequiredField {
    /** The type of check to be performed on the reference field. */
    public final ConditionallyRequiredFieldCheck referenceCheck;
    /** The type of check to be performed on the conditional field. */
    public  ConditionallyRequiredFieldCheck conditionalCheck;
    /** The minimum reference field value if a range check is being performed. */
    public double minReferenceValue;
    /** The maximum reference field value if a range check is being performed. */
    public double maxReferenceValue;
    /** The name of the reference field. */
    String referenceFieldName;
    /** The name of the conditional field. */
    String conditionalFieldName;

    ConditionallyRequiredField (
        String referenceFieldName,
        ConditionallyRequiredFieldCheck referenceCheck,
        String conditionalFieldName,
        ConditionallyRequiredFieldCheck conditionalCheck,
        double minReferenceValue,
        double maxReferenceValue
    ) {
        this.referenceFieldName = referenceFieldName;
        this.referenceCheck = referenceCheck;
        this.conditionalFieldName = conditionalFieldName;
        this.conditionalCheck = conditionalCheck;
        this.minReferenceValue = minReferenceValue;
        this.maxReferenceValue = maxReferenceValue;
    }
}
