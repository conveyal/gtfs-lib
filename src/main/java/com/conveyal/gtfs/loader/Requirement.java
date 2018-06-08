package com.conveyal.gtfs.loader;

/**
 * These are field requirement levels, which should be assigned accordingly to any fields within a {@link Table}
 * to determine if a field is requisite for a specific purpose.
 *
 * TODO: These enum values should be placed in an ordered list or assigned order constants, which allow a generic
 * filtering predicate that returns true for elements "up to and including" a supplied level. Currently, this has a
 * fragmented implementation in {@link Table#editorFields()} and {@link Table#specFields()}. However, it is currently
 * unclear what the levels.order for the types EXTENSION, PROPIETARY, and UNKNOWN ought to be.
 */
public enum Requirement {
    REQUIRED,    // Required by the GTFS spec
    OPTIONAL,    // Optional according to the GTFS spec
    EXTENSION,   // Extension proposed and documented on gtfs-changes
    PROPRIETARY, // Known proprietary extension that is not yet an official proposal
    UNKNOWN,     // Undocumented proprietary extension
    EDITOR       // Editor-specific fields (e.g., pattern_id)
}
