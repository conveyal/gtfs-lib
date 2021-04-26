package com.conveyal.gtfs.loader;

/**
 * These are the values that are checked inline with {@link ConditionalCheckType} to determine if the required
 * conditions have been met.
 */
public class ConditionalRequirement {
    /** The type of check to be performed on the reference field. */
    public ConditionalCheckType referenceCheck;
    /** The minimum reference field value if a range check is being performed. */
    public int minReferenceValue;
    /** The maximum reference field value if a range check is being performed. */
    public int maxReferenceValue;
    /** The type of check to be performed on the conditional field. */
    public ConditionalCheckType conditionalCheck;
    /** The name of the conditional field. */
    public String conditionalFieldName;

    public ConditionalRequirement(
        int minReferenceValue,
        int maxReferenceValue,
        String conditionalFieldName,
        ConditionalCheckType conditionalCheck

    ) {
        this.referenceCheck = ConditionalCheckType.FIELD_IN_RANGE;
        this.minReferenceValue = minReferenceValue;
        this.maxReferenceValue = maxReferenceValue;
        this.conditionalFieldName = conditionalFieldName;
        this.conditionalCheck = conditionalCheck;
    }

    public ConditionalRequirement(String conditionalFieldName) {
        this.referenceCheck = ConditionalCheckType.ROW_COUNT_GREATER_THAN_ONE;
        this.conditionalFieldName = conditionalFieldName;
    }

    public ConditionalRequirement(
        String conditionalFieldName,
        ConditionalCheckType conditionalCheck
    ) {
        this.referenceCheck = ConditionalCheckType.ROW_COUNT_GREATER_THAN_ONE;
        this.conditionalFieldName = conditionalFieldName;
        this.conditionalCheck = conditionalCheck;
    }
}
