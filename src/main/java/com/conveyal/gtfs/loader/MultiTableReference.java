package com.conveyal.gtfs.loader;

import com.google.common.collect.Multimap;

public class MultiTableReference {
    public String fieldName;
    public Multimap<Table, String> referencesPerTable;

    public MultiTableReference(String fieldName, Multimap<Table, String> referencesPerTable) {
        this.fieldName = fieldName;
        this.referencesPerTable = referencesPerTable;
    }
}
