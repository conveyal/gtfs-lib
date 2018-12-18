package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.model.Agency;
import com.conveyal.gtfs.model.Calendar;
import com.conveyal.gtfs.model.CalendarDate;
import com.conveyal.gtfs.model.Entity;
import com.conveyal.gtfs.model.FareAttribute;
import com.conveyal.gtfs.model.FareRule;
import com.conveyal.gtfs.model.FeedInfo;
import com.conveyal.gtfs.model.Frequency;
import com.conveyal.gtfs.model.Pattern;
import com.conveyal.gtfs.model.PatternStop;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.ScheduleException;
import com.conveyal.gtfs.model.ShapePoint;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Transfer;
import com.conveyal.gtfs.model.Trip;
import com.conveyal.gtfs.storage.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.conveyal.gtfs.error.NewGTFSErrorType.DUPLICATE_ID;
import static com.conveyal.gtfs.error.NewGTFSErrorType.REFERENTIAL_INTEGRITY;
import static com.conveyal.gtfs.loader.Requirement.EDITOR;
import static com.conveyal.gtfs.loader.Requirement.EXTENSION;
import static com.conveyal.gtfs.loader.Requirement.OPTIONAL;
import static com.conveyal.gtfs.loader.Requirement.REQUIRED;
import static com.conveyal.gtfs.loader.Requirement.UNKNOWN;


/**
 * This groups a table name with a description of the fields in the table.
 * It can be normative (expressing the specification for a CSV table in GTFS)
 * or descriptive (providing the schema of an RDBMS table).
 *
 * TODO associate Table with EntityPopulator (combine read and write sides)
 */
public class Table {

    private static final Logger LOG = LoggerFactory.getLogger(Table.class);

    public final String name;

    final Class<? extends Entity> entityClass;

    final Requirement required;

    public final Field[] fields;
    /** Determines whether cascading delete is restricted. Defaults to false (i.e., cascade delete is allowed) */
    private boolean cascadeDeleteRestricted = false;
    /** An update to the parent table will trigger an update to this table if parent has nested entities. */
    private Table parentTable;
    /** When snapshotting a table for editor use, this indicates whether a primary key constraint should be added to ID. */
    private boolean usePrimaryKey = false;
    /** Indicates whether the table has unique key field. */
    private boolean hasUniqueKeyField = true;

    public Table (String name, Class<? extends Entity> entityClass, Requirement required, Field... fields) {
        // TODO: verify table name is OK for use in constructing dynamic SQL queries
        this.name = name;
        this.entityClass = entityClass;
        this.required = required;
        this.fields = fields;
    }

    public static final Table AGENCY = new Table("agency", Agency.class, REQUIRED,
        new StringField("agency_id",  OPTIONAL), // FIXME? only required if there are more than one
        new StringField("agency_name",  REQUIRED),
        new URLField("agency_url",  REQUIRED),
        new StringField("agency_timezone",  REQUIRED), // FIXME new field type for time zones?
        new StringField("agency_lang", OPTIONAL), // FIXME new field type for languages?
        new StringField("agency_phone",  OPTIONAL),
        new URLField("agency_branding_url",  OPTIONAL),
        new URLField("agency_fare_url",  OPTIONAL),
        new StringField("agency_email",  OPTIONAL) // FIXME new field type for emails?
    ).restrictDelete().addPrimaryKey();

    // The GTFS spec says this table is required, but in practice it is not required if calendar_dates is present.
    public static final Table CALENDAR = new Table("calendar", Calendar.class, OPTIONAL,
        new StringField("service_id",  REQUIRED),
        new IntegerField("monday", REQUIRED, 0, 1),
        new IntegerField("tuesday", REQUIRED, 0, 1),
        new IntegerField("wednesday", REQUIRED, 0, 1),
        new IntegerField("thursday", REQUIRED, 0, 1),
        new IntegerField("friday", REQUIRED, 0, 1),
        new IntegerField("saturday", REQUIRED, 0, 1),
        new IntegerField("sunday", REQUIRED, 0, 1),
        new DateField("start_date", REQUIRED),
        new DateField("end_date", REQUIRED),
        // Editor-specific field
        new StringField("description", EDITOR)
    ).restrictDelete().addPrimaryKey();

    public static final Table SCHEDULE_EXCEPTIONS = new Table("schedule_exceptions", ScheduleException.class, EDITOR,
            new StringField("name", REQUIRED), // FIXME: This makes name the key field...
            // FIXME: Change to DateListField
            new DateListField("dates", REQUIRED),
            new ShortField("exemplar", REQUIRED, 9),
            new StringListField("custom_schedule", OPTIONAL).isReferenceTo(CALENDAR),
            new StringListField("added_service", OPTIONAL).isReferenceTo(CALENDAR),
            new StringListField("removed_service", OPTIONAL).isReferenceTo(CALENDAR)
    );

    public static final Table CALENDAR_DATES = new Table("calendar_dates", CalendarDate.class, OPTIONAL,
        new StringField("service_id", REQUIRED),
        new DateField("date", REQUIRED),
        new IntegerField("exception_type", REQUIRED, 1, 2)
    ).keyFieldIsNotUnique();

    public static final Table FARE_ATTRIBUTES = new Table("fare_attributes", FareAttribute.class, OPTIONAL,
        new StringField("fare_id", REQUIRED),
        new DoubleField("price", REQUIRED, 0.0, Double.MAX_VALUE, 2),
        new CurrencyField("currency_type", REQUIRED),
        new ShortField("payment_method", REQUIRED, 1),
        new ShortField("transfers", REQUIRED, 2).permitEmptyValue(),
        new StringField("agency_id", OPTIONAL), // FIXME? only required if there are more than one
        new IntegerField("transfer_duration", OPTIONAL)
    ).addPrimaryKey();

    // FIXME: Should we add some constraint on number of rows that this table has? Perhaps this is a GTFS editor specific
    // feature.
    public static final Table FEED_INFO = new Table("feed_info", FeedInfo.class, OPTIONAL,
        new StringField("feed_publisher_name", REQUIRED),
        new URLField("feed_publisher_url", REQUIRED),
        new LanguageField("feed_lang", REQUIRED),
        new DateField("feed_start_date", OPTIONAL),
        new DateField("feed_end_date", OPTIONAL),
        new StringField("feed_version", OPTIONAL),
        // Editor-specific field that represents default route values for use in editing.
        new ColorField("default_route_color", EDITOR),
        // FIXME: Should the route type max value be equivalent to GTFS spec's max?
        new IntegerField("default_route_type", EDITOR, 999)
    ).keyFieldIsNotUnique();

    public static final Table ROUTES = new Table("routes", Route.class, REQUIRED,
        new StringField("route_id",  REQUIRED),
        new StringField("agency_id",  OPTIONAL).isReferenceTo(AGENCY),
        new StringField("route_short_name",  OPTIONAL), // one of short or long must be provided
        new StringField("route_long_name",  OPTIONAL),
        new StringField("route_desc",  OPTIONAL),
        // Max route type according to the GTFS spec is 7; however, there is a GTFS proposal that could see this 
        // max value grow to around 1800: https://groups.google.com/forum/#!msg/gtfs-changes/keT5rTPS7Y0/71uMz2l6ke0J
        new IntegerField("route_type", REQUIRED, 1800),
        new URLField("route_url",  OPTIONAL),
        new URLField("route_branding_url",  OPTIONAL),
        new ColorField("route_color",  OPTIONAL), // really this is an int in hex notation
        new ColorField("route_text_color",  OPTIONAL),
        // Editor fields below.
        new ShortField("publicly_visible", EDITOR, 1),
        // wheelchair_accessible is an exemplar field applied to all trips on a route.
        new ShortField("wheelchair_accessible", EDITOR, 2).permitEmptyValue(),
        new IntegerField("route_sort_order", OPTIONAL, 0, Integer.MAX_VALUE),
        // Status values are In progress (0), Pending approval (1), and Approved (2).
        new ShortField("status", EDITOR,  2)
    ).addPrimaryKey();

    public static final Table FARE_RULES = new Table("fare_rules", FareRule.class, OPTIONAL,
            new StringField("fare_id", REQUIRED).isReferenceTo(FARE_ATTRIBUTES),
            new StringField("route_id", OPTIONAL).isReferenceTo(ROUTES),
            // FIXME: referential integrity check for zone_id for below three fields?
            new StringField("origin_id", OPTIONAL),
            new StringField("destination_id", OPTIONAL),
            new StringField("contains_id", OPTIONAL))
            .withParentTable(FARE_ATTRIBUTES)
            .addPrimaryKey().keyFieldIsNotUnique();

    public static final Table SHAPES = new Table("shapes", ShapePoint.class, OPTIONAL,
            new StringField("shape_id", REQUIRED),
            new IntegerField("shape_pt_sequence", REQUIRED),
            new DoubleField("shape_pt_lat", REQUIRED, -80, 80, 6),
            new DoubleField("shape_pt_lon", REQUIRED, -180, 180, 6),
            new DoubleField("shape_dist_traveled", OPTIONAL, 0, Double.POSITIVE_INFINITY, -1),
            // Editor-specific field that represents a shape point's behavior in UI.
            // 0 - regular shape point
            // 1 - user-designated anchor point (handle with which the user can manipulate shape)
            // 2 - stop-projected point (dictates the value of shape_dist_traveled for a pattern stop)
            new ShortField("point_type", EDITOR, 2)
    );

    public static final Table PATTERNS = new Table("patterns", Pattern.class, OPTIONAL,
            new StringField("pattern_id", REQUIRED),
            new StringField("route_id", REQUIRED).isReferenceTo(ROUTES),
            new StringField("name", OPTIONAL),
            // Editor-specific fields.
            // direction_id and shape_id are exemplar fields applied to all trips for a pattern.
            new ShortField("direction_id", EDITOR, 1),
            new ShortField("use_frequency", EDITOR, 1),
            new StringField("shape_id", EDITOR).isReferenceTo(SHAPES)
    ).addPrimaryKey();

    public static final Table STOPS = new Table("stops", Stop.class, REQUIRED,
        new StringField("stop_id",  REQUIRED),
        new StringField("stop_code",  OPTIONAL),
        new StringField("stop_name",  REQUIRED),
        new StringField("stop_desc",  OPTIONAL),
        new DoubleField("stop_lat", REQUIRED, -80, 80, 6),
        new DoubleField("stop_lon", REQUIRED, -180, 180, 6),
        new StringField("zone_id",  OPTIONAL),
        new URLField("stop_url",  OPTIONAL),
        new ShortField("location_type", OPTIONAL, 2),
        // FIXME: Need self-reference check during referential integrity check
        new StringField("parent_station",  OPTIONAL), //.isReferenceToSelf()
        new StringField("stop_timezone",  OPTIONAL),
        new ShortField("wheelchair_boarding", OPTIONAL, 2)
    ).restrictDelete().addPrimaryKey();

    public static final Table PATTERN_STOP = new Table("pattern_stops", PatternStop.class, OPTIONAL,
            new StringField("pattern_id", REQUIRED).isReferenceTo(PATTERNS),
            new IntegerField("stop_sequence", REQUIRED, 0, Integer.MAX_VALUE),
            // FIXME: Do we need an index on stop_id?
            new StringField("stop_id", REQUIRED).isReferenceTo(STOPS),
            // Editor-specific fields
            new IntegerField("default_travel_time", EDITOR,0, Integer.MAX_VALUE),
            new IntegerField("default_dwell_time", EDITOR, 0, Integer.MAX_VALUE),
            new IntegerField("drop_off_type", EDITOR, 2),
            new IntegerField("pickup_type", EDITOR, 2),
            new DoubleField("shape_dist_traveled", EDITOR, 0, Double.POSITIVE_INFINITY, -1),
            new ShortField("timepoint", EDITOR, 1)
    ).withParentTable(PATTERNS);

    public static final Table TRANSFERS = new Table("transfers", Transfer.class, OPTIONAL,
            // FIXME: Do we need an index on from_ and to_stop_id
            new StringField("from_stop_id", REQUIRED).isReferenceTo(STOPS),
            new StringField("to_stop_id", REQUIRED).isReferenceTo(STOPS),
            new StringField("transfer_type", REQUIRED),
            new StringField("min_transfer_time", OPTIONAL))
            .addPrimaryKey()
            .keyFieldIsNotUnique();

    public static final Table TRIPS = new Table("trips", Trip.class, REQUIRED,
        new StringField("trip_id",  REQUIRED),
        new StringField("route_id",  REQUIRED).isReferenceTo(ROUTES).indexThisColumn(),
        // FIXME: Should this also optionally reference CALENDAR_DATES?
        // FIXME: Do we need an index on service_id
        new StringField("service_id",  REQUIRED).isReferenceTo(CALENDAR),
        new StringField("trip_headsign",  OPTIONAL),
        new StringField("trip_short_name",  OPTIONAL),
        new ShortField("direction_id", OPTIONAL, 1),
        new StringField("block_id",  OPTIONAL),
        new StringField("shape_id",  OPTIONAL).isReferenceTo(SHAPES),
        new ShortField("wheelchair_accessible", OPTIONAL, 2),
        new ShortField("bikes_allowed", OPTIONAL, 2),
        // Editor-specific fields below.
        new StringField("pattern_id", EDITOR).isReferenceTo(PATTERNS)
    ).addPrimaryKey();

    // Must come after TRIPS and STOPS table to which it has references
    public static final Table STOP_TIMES = new Table("stop_times", StopTime.class, REQUIRED,
            new StringField("trip_id", REQUIRED).isReferenceTo(TRIPS),
            new IntegerField("stop_sequence", REQUIRED, 0, Integer.MAX_VALUE),
            // FIXME: Do we need an index on stop_id
            new StringField("stop_id", REQUIRED).isReferenceTo(STOPS),
//                    .indexThisColumn(),
            // TODO verify that we have a special check for arrival and departure times first and last stop_time in a trip, which are reqiured
            new TimeField("arrival_time", OPTIONAL),
            new TimeField("departure_time", OPTIONAL),
            new StringField("stop_headsign", OPTIONAL),
            new ShortField("pickup_type", OPTIONAL, 2),
            new ShortField("drop_off_type", OPTIONAL, 2),
            new DoubleField("shape_dist_traveled", OPTIONAL, 0, Double.POSITIVE_INFINITY, 2),
            new ShortField("timepoint", OPTIONAL, 1),
            new IntegerField("fare_units_traveled", EXTENSION) // OpenOV NL extension
    ).withParentTable(TRIPS);

    // Must come after TRIPS table to which it has a reference
    public static final Table FREQUENCIES = new Table("frequencies", Frequency.class, OPTIONAL,
            new StringField("trip_id", REQUIRED).isReferenceTo(TRIPS),
            new TimeField("start_time", REQUIRED),
            new TimeField("end_time", REQUIRED),
            new IntegerField("headway_secs", REQUIRED, 20, 60*60*2),
            new IntegerField("exact_times", OPTIONAL, 1))
            .withParentTable(TRIPS)
            .keyFieldIsNotUnique();

    /** List of tables in order needed for checking referential integrity during load stage. */
    public static final Table[] tablesInOrder = {
            AGENCY,
            CALENDAR,
            SCHEDULE_EXCEPTIONS,
            CALENDAR_DATES,
            FARE_ATTRIBUTES,
            FEED_INFO,
            ROUTES,
            FARE_RULES,
            PATTERNS,
            SHAPES,
            STOPS,
            PATTERN_STOP,
            TRANSFERS,
            TRIPS,
            STOP_TIMES,
            FREQUENCIES
    };

    /**
     * Fluent method that restricts deletion of an entity in this table if there are references to it elsewhere. For
     * example, a calendar that has trips referencing it must not be deleted.
     */
    public Table restrictDelete () {
        this.cascadeDeleteRestricted = true;
        return this;
    }

    /**
     * Fluent method to de-set the
     */
    private Table keyFieldIsNotUnique() {
        this.hasUniqueKeyField = false;
        return this;
    }

    /**
     * Fluent method that indicates that the integer ID field should be made a primary key. This should generally only
     * be used for tables that would ever need to be queried on the unique integer ID (which represents row number for
     * tables directly after csv load). For example, we may need to query for a stop or route by unique ID in order to
     * update or delete it. (Whereas querying for a specific stop time vs. a set of stop times would rarely if ever be
     * needed.)
     */
    public Table addPrimaryKey () {
        this.usePrimaryKey = true;
        return this;
    }

    /**
     * Registers the table with a parent table. When updates are made to the parent table, updates to child entities
     * nested in the JSON string will be made. For example, pattern stops and shape points use this method to point to
     * the pattern table as their parent. Currently, an update to either of these child tables must be made by way of
     * a pattern update with nested array pattern_stops and/or shapes. This is due in part to the historical editor
     * data structure in mapdb which allowed for storing a list of objects as a table field. It also (TBD) may be useful
     * for ensuring data integrity and avoiding potentially risky partial updates. FIXME This needs further investigation.
     *
     * FIXME: Perhaps this logic should be removed from the Table class and explicitly stated in the editor/writer
     * classes.
     */
    private Table withParentTable(Table parentTable) {
        this.parentTable = parentTable;
        return this;
    }

    /**
     * Get only those fields included in the official GTFS specification for this table or used by the editor.
     */
    public List<Field> editorFields() {
        List<Field> editorFields = new ArrayList<>();
        for (Field f : fields) if (f.requirement == REQUIRED || f.requirement == OPTIONAL || f.requirement == EDITOR) {
            editorFields.add(f);
        }
        return editorFields;
    }

    /**
     * Get only those fields marked as required in the official GTFS specification for this table.
     */
    public List<Field> requiredFields () {
        // Filter out fields not used in editor (i.e., extension fields).
        List<Field> requiredFields = new ArrayList<>();
        for (Field f : fields) if (f.requirement == REQUIRED) requiredFields.add(f);
        return requiredFields;
    }

    /**
     * Get only those fields included in the official GTFS specification for this table, i.e., filter out fields used
     * in the editor or extensions.
     */
    public List<Field> specFields () {
        List<Field> specFields = new ArrayList<>();
        for (Field f : fields) if (f.requirement == REQUIRED || f.requirement == OPTIONAL) specFields.add(f);
        return specFields;
    }

    public boolean isCascadeDeleteRestricted() {
        return cascadeDeleteRestricted;
    }


    public boolean createSqlTable(Connection connection) {
        return createSqlTable(connection, null, false, null);
    }

    public boolean createSqlTable(Connection connection, boolean makeIdSerial) {
        return createSqlTable(connection, null, makeIdSerial, null);
    }

    public boolean createSqlTable(Connection connection, String namespace, boolean makeIdSerial) {
        return createSqlTable(connection, namespace, makeIdSerial, null);
    }

    /**
     * Create an SQL table with all the fields specified by this table object,
     * plus an integer CSV line number field in the first position.
     */
    public boolean createSqlTable (Connection connection, String namespace, boolean makeIdSerial, String[] primaryKeyFields) {
        // Optionally join namespace and name to create full table name if namespace is not null (i.e., table object is
        // a spec table).
        String tableName = namespace != null ? String.join(".", namespace, name) : name;
        String fieldDeclarations = Arrays.stream(fields)
                .map(Field::getSqlDeclaration)
                .collect(Collectors.joining(", "));
        if (primaryKeyFields != null) {
            fieldDeclarations += String.format(", primary key (%s)", String.join(", ", primaryKeyFields));
        }
        String dropSql = String.format("drop table if exists %s", tableName);
        // Adding the unlogged keyword gives about 12 percent speedup on loading, but is non-standard.
        String idFieldType = makeIdSerial ? "serial" : "bigint";
        String createSql = String.format("create table %s (id %s not null, %s)", tableName, idFieldType, fieldDeclarations);
        try {
            Statement statement = connection.createStatement();
            LOG.info(dropSql);
            statement.execute(dropSql);
            LOG.info(createSql);
            return statement.execute(createSql);
        } catch (Exception ex) {
            throw new StorageException(ex);
        }
    }

    /**
     * Create an SQL table that will insert a value into all the fields specified by this table object,
     * plus an integer CSV line number field in the first position.
     */
    public String generateInsertSql () {
        return generateInsertSql(null, false);
    }

    public String generateInsertSql (boolean setDefaultId) {
        return generateInsertSql(null, setDefaultId);
    }

    /**
     * Create SQL string for use in insert statement. Note, this filters table's fields to only those used in editor.
     */
    public String generateInsertSql (String namespace, boolean setDefaultId) {
        String tableName = namespace == null
                ? name
                : String.join(".", namespace, name);
        String questionMarks = String.join(", ", Collections.nCopies(editorFields().size(), "?"));
        String joinedFieldNames = commaSeparatedNames(editorFields());
        String idValue = setDefaultId ? "DEFAULT" : "?";
        return String.format("insert into %s (id, %s) values (%s, %s)", tableName, joinedFieldNames, idValue, questionMarks);
    }

    /**
     * Join a list of fields with a comma + space separator.
     */
    public static String commaSeparatedNames(List<Field> fieldsToJoin) {
        return commaSeparatedNames(fieldsToJoin, null, false);
    }

    /**
     * Prepend a prefix string to each field and join them with a comma + space separator.
     * Also, if an export to GTFS is being performed, certain fields need a translation from the database format to the
     * GTFS format.  Otherwise, the fields are assumed to be asked in order to do a database-to-database export and so
     * the verbatim values of the fields are needed.
     */
    public static String commaSeparatedNames(List<Field> fieldsToJoin, String prefix, boolean csvOutput) {
        return fieldsToJoin.stream()
                // NOTE: This previously only prefixed fields that were foreign refs or key fields. However, this
                // caused an issue where shared fields were ambiguously referenced in a select query (specifically,
                // wheelchair_accessible in routes and trips). So this filter has been removed.
                .map(f -> f.getColumnExpression(prefix, csvOutput))
                .collect(Collectors.joining(", "));
    }

    // FIXME: Add table method that sets a field parameter based on field name and string value?
//    public void setParameterByName (PreparedStatement preparedStatement, String fieldName, String value) {
//        Field field = getFieldForName(fieldName);
//        int editorFieldIndex = editorFields().indexOf(field);
//        field.setParameter(preparedStatement, editorFieldIndex, value);
//    }

    /**
     * Create SQL string for use in update statement. Note, this filters table's fields to only those used in editor.
     */
    public String generateUpdateSql (String namespace, int id) {
        // Collect field names for string joining from JsonObject.
        String joinedFieldNames = editorFields().stream()
                // If updating, add suffix for use in set clause
                .map(field -> field.name + " = ?")
                .collect(Collectors.joining(", "));

        String tableName = namespace == null ? name : String.join(".", namespace, name);
        return String.format("update %s set %s where id = %d", tableName, joinedFieldNames, id);
    }

    /**
     * Generate select all SQL string. The minimum requirement parameter is used to determine which fields ought to be
     * included in the select statement. For example, if "OPTIONAL" is passed in, both optional and required fields
     * are included in the select. If "EDITOR" is the minimum requirement, editor, optional, and required fields will
     * all be included.
     */
    public String generateSelectSql (String namespace, Requirement minimumRequirement) {
        String fieldsString;
        String tableName = String.join(".", namespace, name);
        String fieldPrefix = tableName + ".";
        if (minimumRequirement.equals(EDITOR)) {
            fieldsString = commaSeparatedNames(editorFields(), fieldPrefix, true);
        } else if (minimumRequirement.equals(OPTIONAL)) {
            fieldsString = commaSeparatedNames(specFields(), fieldPrefix, true);
        } else if (minimumRequirement.equals(REQUIRED)) {
            fieldsString = commaSeparatedNames(requiredFields(), fieldPrefix, true);
        } else fieldsString = "*";
        return String.format("select %s from %s", fieldsString, tableName);
    }

    /**
     * Generate a select statement from the columns that actually exist in the database table.  This method is intended
     * to be used when exporting to a GTFS and eventually generates the select all with each individual field and
     * applicable transformations listed out.
     */
    public String generateSelectAllExistingFieldsSql(Connection connection, String namespace) throws SQLException {
        // select all columns from table
        // FIXME This is postgres-specific and needs to be made generic for non-postgres databases.
        PreparedStatement statement = connection.prepareStatement(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ?"
        );
        statement.setString(1, namespace);
        statement.setString(2, name);
        ResultSet result = statement.executeQuery();

        // get result and add fields that are defined in this table
        List<Field> existingFields = new ArrayList<>();
        while (result.next()) {
            String columnName = result.getString(1);
            existingFields.add(getFieldForName(columnName));
        }

        String tableName = String.join(".", namespace, name);
        String fieldPrefix = tableName + ".";
        return String.format(
            "select %s from %s", commaSeparatedNames(existingFields, fieldPrefix, true), tableName
        );
    }

    public String generateJoinSql (Table joinTable, String namespace, String fieldName, boolean prefixTableName) {
        return generateJoinSql(null, joinTable, null, namespace, fieldName, prefixTableName);
    }

    public String generateJoinSql (String optionalSelect, Table joinTable, String namespace) {
        return generateJoinSql(optionalSelect, joinTable, null, namespace, null, true);
    }

    public String generateJoinSql (Table joinTable, String namespace) {
        return generateJoinSql(null, joinTable, null, namespace, null, true);
    }

    /**
     * Constructs a join clause to use in conjunction with {@link #generateJoinSql}. By default the join type is "INNER
     * JOIN" and the join field is whatever the table instance's key field is. Both of those defaults can be overridden
     * with the other overloaded methods.
     * @param optionalSelect optional select query to pre-select the join table
     * @param joinTable the Table to join with
     * @param joinType type of join (e.g., INNER JOIN, OUTER LEFT JOIN, etc.)
     * @param namespace feedId (or schema prefix)
     * @param fieldName the field to join on (default's to key field)
     * @param prefixTableName whether to prefix this table's name with the schema (helpful if this join follows a
     *                        previous join that renamed the table by dropping the schema/namespace
     * @return a fully formed join clause that can be appended to a select statement
     */
    public String generateJoinSql (String optionalSelect, Table joinTable, String joinType, String namespace, String fieldName, boolean prefixTableName) {
        if (fieldName == null) {
            // Default to key field if field name is not specified
            fieldName = getKeyFieldName();
        }
        if (joinType == null) {
            // Default to INNER JOIN if no join type provided
            joinType = "INNER JOIN";
        }
        String joinTableQuery;
        String joinTableName;
        if (optionalSelect != null) {
            // If a pre-select query is provided for the join table, the join table name must not contain the namespace
            // prefix.
            joinTableName = joinTable.name;
            joinTableQuery = String.format("(%s) %s", optionalSelect, joinTableName);
        } else {
            // Otherwise, set both the join "query" and the table name to the standard "namespace.table_name"
            joinTableQuery = joinTableName = String.format("%s.%s", namespace, joinTable.name);
        }
        // If prefix table name is set to false, skip prefixing the table name.
        String tableName = prefixTableName ? String.format("%s.%s", namespace, this.name) :this.name;
        // Construct the join clause. A sample might appear like:
        // "INNER JOIN schema.join_table ON schema.this_table.field_name = schema.join_table.field_name"
        // OR if optionalSelect is specified
        // "INNER JOIN (select * from schema.join_table where x = 'y') join_table ON schema.this_table.field_name = join_table.field_name"
        return String.format("%s %s ON %s.%s = %s.%s",
                joinType,
                joinTableQuery,
                tableName, fieldName,
                joinTableName, fieldName);
    }

    public String generateDeleteSql (String namespace) {
        return generateDeleteSql(namespace, null);
    }

    /**
     * Generate delete SQL string.
     */
    public String generateDeleteSql (String namespace, String fieldName) {
        String whereField = fieldName == null ? "id" : fieldName;
        return String.format("delete from %s where %s = ?", String.join(".", namespace, name), whereField);
    }

    /**
     * @param name a column name from the header of a CSV file
     * @return the Field object from this table with the given name. If there is no such field defined, create
     * a new Field object for this name.
     */
    public Field getFieldForName(String name) {
        int index = getFieldIndex(name);
        if (index >= 0) return fields[index];
        LOG.warn("Unrecognized header {}. Treating it as a proprietary string field.", name);
        return new StringField(name, UNKNOWN);
    }

    /**
     * Gets the key field for the table. Calling this on a table that has no key field is meaningless. WARNING: this
     * MUST be called on a spec table (i.e., one of the constant tables defined in this class). Otherwise, it
     * could return a non-key field.
     *
     * FIXME: Should this return null if hasUniqueKeyField is false? Not sure what might break if we change this...
     */
    public String getKeyFieldName () {
        // FIXME: If the table is constructed from fields found in a GTFS file, the first field is not guaranteed to be
        // the key field.
        return fields[0].name;
    }

    /**
     * Returns field name that defines order for grouped entities. WARNING: this MUST be called on a spec table (i.e.,
     * one of the constant tables defined in this class). Otherwise, it could return null even if the table has an order
     * field defined.
     */
    public String getOrderFieldName () {
        String name = fields[1].name;
        if (name.contains("_sequence")) return name;
        else return null;
    }

    /**
     * Gets index fields for the spec table. WARNING: this MUST be called on a spec table (i.e., one of the constant
     * tables defined in this class). Otherwise, it could return fields that should not be indexed.
     * @return
     */
    public String getIndexFields() {
        String orderFieldName = getOrderFieldName();
        if (orderFieldName == null) return getKeyFieldName();
        else return String.join(",", getKeyFieldName(), orderFieldName);
    }

    public Class<? extends Entity> getEntityClass() {
        return entityClass;
    }


    /**
     * Finds the index of the field given a string name.
     * @return the index of the field or -1 if no match is found
     */
    public int getFieldIndex (String name) {
        // Linear search, assuming a small number of fields per table.
        for (int i = 0; i < fields.length; i++) if (fields[i].name.equals(name)) return i;
        return -1;
    }

    /**
     * Whether a field with the provided name exists in the table's list of fields.
     */
    public boolean hasField (String name) {
        return getFieldIndex(name) != -1;
    }

    public boolean isRequired () {
        return required == REQUIRED;
    }

    /**
     * Create indexes for table using shouldBeIndexed(), key field, and/or sequence field. WARNING: this MUST be called
     * on a spec table (i.e., one of the constant tables defined in this class). Otherwise, the getIndexFields method
     * could return fields that should not be indexed.
     * FIXME: add foreign reference indexes?
     */
    public void createIndexes(Connection connection, String namespace) throws SQLException {
        if ("agency".equals(name) || "feed_info".equals(name)) {
            // Skip indexing for the small tables that have so few records that indexes are unlikely to
            // improve query performance or that are unlikely to be joined to other tables. NOTE: other tables could be
            // added here in the future as needed.
            LOG.info("Skipping indexes for {} table", name);
            return;
        }
        LOG.info("Indexing {}...", name);
        String tableName;
        if (namespace == null) {
            throw new IllegalStateException("Schema namespace must be provided!");
        } else {
            // Construct table name with prefixed namespace (and account for whether it already ends in a period).
            tableName = namespace.endsWith(".") ? namespace + name : String.join(".", namespace, name);
        }
        // We determine which columns should be indexed based on field order in the GTFS spec model table.
        // Not sure that's a good idea, this could use some abstraction. TODO getIndexColumns() on each table.
        String indexColumns = getIndexFields();
        // TODO verify referential integrity and uniqueness of keys
        // TODO create primary key and fall back on plain index (consider not null & unique constraints)
        // TODO use line number as primary key
        // Note: SQLITE requires specifying a name for indexes.
        String indexName = String.join("_", tableName.replace(".", "_"), "idx");
        String indexSql = String.format("create index %s on %s (%s)", indexName, tableName, indexColumns);
        //String indexSql = String.format("alter table %s add primary key (%s)", tableName, indexColumns);
        LOG.info(indexSql);
        connection.createStatement().execute(indexSql);
        // TODO add foreign key constraints, and recover recording errors as needed.

        // More indexing
        // TODO integrate with the above indexing code, iterating over a List<String> of index column expressions
        for (Field field : fields) {
            if (field.shouldBeIndexed()) {
                Statement statement = connection.createStatement();
                String fieldIndex = String.join("_", tableName.replace(".", "_"), field.name, "idx");
                String sql = String.format("create index %s on %s (%s)", fieldIndex, tableName, field.name);
                LOG.info(sql);
                statement.execute(sql);
            }
        }
    }

    /**
     * Creates a SQL table from the table to clone. This uses the SQL syntax "create table x as y" not only copies the
     * table structure, but also the data from the original table. Creating table indexes is not handled by this method.
     */
    public boolean createSqlTableFrom(Connection connection, String tableToClone) {
        long startTime = System.currentTimeMillis();
        try {
            Statement statement = connection.createStatement();
            // Drop target table to avoid a conflict.
            String dropSql = String.format("drop table if exists %s", name);
            LOG.info(dropSql);
            statement.execute(dropSql);
            if (tableToClone.endsWith("stop_times")) {
                normalizeAndCloneStopTimes(statement, name, tableToClone);
            } else {
                // Adding the unlogged keyword gives about 12 percent speedup on loading, but is non-standard.
                // FIXME: Which create table operation is more efficient?
                String createTableAsSql = String.format("create table %s as table %s", name, tableToClone);
                // Create table in the image of the table we're copying (indexes are not included).
                LOG.info(createTableAsSql);
                statement.execute(createTableAsSql);
            }
            applyAutoIncrementingSequence(statement);
            // FIXME: Is there a need to add primary key constraint here?
            if (usePrimaryKey) {
                // Add primary key to ID column for any tables that require it.
                String addPrimaryKeySql = String.format("ALTER TABLE %s ADD PRIMARY KEY (id)", name);
                LOG.info(addPrimaryKeySql);
                statement.execute(addPrimaryKeySql);
            }
            return true;
        } catch (SQLException ex) {
            LOG.error("Error cloning table {}: {}", name, ex.getSQLState());
            LOG.error("details: ", ex);
            try {
                connection.rollback();
                // It is likely that if cloning the table fails, the reason was that the table did not already exist.
                // Try to create the table here from scratch.
                // FIXME: Maybe we should check that the reason the clone failed was that the table already exists.
                createSqlTable(connection, true);
                return true;
            } catch (SQLException e) {
                e.printStackTrace();
                return false;
            }
        } finally {
            LOG.info("Cloned table {} as {} in {} ms", tableToClone, name, System.currentTimeMillis() - startTime);
        }
    }

    /**
     *  Normalize stop sequences for stop times table so that sequences are all zero-based and increment
     by one. This ensures that sequence values for stop_times and pattern_stops are not initially out
     of sync for feeds imported into the editor.

     NOTE: This happens here instead of as part of post-processing because it's much faster overall to perform
     this as an INSERT vs. an UPDATE. It also needs to be done before creating table indexes. There may be some
     messiness here as far as using the column metadata to perform the SELECT query with the correct column names, but
     it is about an order of magnitude faster than the UPDATE approach.

     For example, with the Bronx bus feed, the UPDATE approach took 53 seconds on an un-normalized table (i.e., snapshotting
     a table from a direct GTFS load), but it only takes about 8 seconds with the INSERT approach. Additionally, this
     INSERT approach seems to dramatically cut down on the time needed for indexing large tables.
     */
    private void normalizeAndCloneStopTimes(Statement statement, String name, String tableToClone) throws SQLException {
        // Create table with matching columns first and then insert all rows with a special select query that
        // normalizes the stop sequences before inserting.
        // "Create table like" can optionally include indexes, but we want to avoid creating the indexes beforehand
        // because this will slow down our massive insert for stop times.
        String createTableLikeSql = String.format("create table %s (like %s)", name, tableToClone);
        LOG.info(createTableLikeSql);
        statement.execute(createTableLikeSql);
        long normalizeStartTime = System.currentTimeMillis();
        LOG.info("Normalizing stop sequences");
        // First get the column names (to account for any non-standard fields that may be present)
        List<String> columns = new ArrayList<>();
        ResultSet resultSet = statement.executeQuery(String.format("select * from %s limit 1", tableToClone));
        ResultSetMetaData metadata = resultSet.getMetaData();
        int nColumns = metadata.getColumnCount();
        for (int i = 1; i <= nColumns; i++) {
            columns.add(metadata.getColumnName(i));
        }
        // Replace stop sequence column with the normalized sequence values.
        columns.set(columns.indexOf("stop_sequence"), "-1 + row_number() over (partition by trip_id order by stop_sequence) as stop_sequence");
        String insertAllSql = String.format("insert into %s (select %s from %s)", name, String.join(", ", columns), tableToClone);
        LOG.info(insertAllSql);
        statement.execute(insertAllSql);
        LOG.info("Normalized stop times sequences in {} ms", System.currentTimeMillis() - normalizeStartTime);
    }

    /**
     * Make id column serial and set the next value based on the current max value. This is intended to operate on
     * existing statement/connection and should not be applied to a table has been created with a serial (i.e., auto-
     * incrementing ID. This code is derived from https://stackoverflow.com/a/9490532/915811
     */
    private void applyAutoIncrementingSequence(Statement statement) throws SQLException {
        String selectMaxSql = String.format("SELECT MAX(id) + 1 FROM %s", name);

        int maxID = 0;
        LOG.info(selectMaxSql);
        statement.execute(selectMaxSql);
        ResultSet maxIdResult = statement.getResultSet();
        if (maxIdResult.next()) {
            maxID = maxIdResult.getInt(1);
        }
        // Set default max ID to 1 (the start value cannot be less than MINVALUE 1)
        // FIXME: Skip sequence creation if maxID = 1?
        if (maxID < 1) {
            maxID = 1;
        }

        String sequenceName = name + "_id_seq";
        String createSequenceSql = String.format("CREATE SEQUENCE %s START WITH %d", sequenceName, maxID);
        LOG.info(createSequenceSql);
        statement.execute(createSequenceSql);

        String alterColumnNextSql = String.format("ALTER TABLE %s ALTER COLUMN id SET DEFAULT nextval('%s')", name, sequenceName);
        LOG.info(alterColumnNextSql);
        statement.execute(alterColumnNextSql);
        String alterColumnNotNullSql = String.format("ALTER TABLE %s ALTER COLUMN id SET NOT NULL", name);
        LOG.info(alterColumnNotNullSql);
        statement.execute(alterColumnNotNullSql);
    }

    public Table getParentTable() {
        return parentTable;
    }

    /**
     * During table load, checks the uniqueness of the entity ID and that references are valid. These references are
     * stored in the provided reference tracker. Any non-unique IDs or invalid references will store an error.
     */
    public void checkReferencesAndUniqueness(String keyValue, int lineNumber, Field field, String string, JdbcGtfsLoader.ReferenceTracker referenceTracker) {
        // Store field-scoped transit ID for referential integrity check. (Note, entity scoping doesn't work here because
        // we need to cross-check multiple entity types for valid references, e.g., stop times and trips both share trip
        // id.)
        String keyField = getKeyFieldName();
        String orderField = getOrderFieldName();
        // If table has no unique key field (e.g., calendar_dates or transfers), there is no need to check for
        // duplicates. If it has an order field, that order field should supersede the key field as the "unique" field.
        String uniqueKeyField = !hasUniqueKeyField ? null : orderField != null ? orderField : keyField;
        String transitId = String.join(":", keyField, keyValue);

        // If the field is optional and there is no value present, skip check.
        if (!field.isRequired() && "".equals(string)) return;

        // First, handle referential integrity check.
        boolean isOrderField = field.name.equals(orderField);
        if (field.isForeignReference()) {
            // Check referential integrity if applicable
            String referenceField = field.referenceTable.getKeyFieldName();
            String referenceTransitId = String.join(":", referenceField, string);

            if (!referenceTracker.transitIds.contains(referenceTransitId)) {
                // If the reference tracker does not contain
                NewGTFSError referentialIntegrityError = NewGTFSError.forLine(
                        this, lineNumber, REFERENTIAL_INTEGRITY, referenceTransitId)
                        .setEntityId(keyValue);
                if (isOrderField) {
                    // If the field is an order field, set the sequence for the new error.
                    referentialIntegrityError.setSequence(string);
                }
                referenceTracker.errorStorage.storeError(referentialIntegrityError);
            }
        }

        // These hold references to the set of IDs to check for duplicates and the ID to check. These depend on
        // whether an order field is part of the "unique ID."
        Set<String> listOfUniqueIds = referenceTracker.transitIds;
        String uniqueId = transitId;

        // There is no need to check for duplicate IDs if the field is a foreign reference.
        if (field.isForeignReference()) return;
        // Next, check that the ID is table-unique.
        if (field.name.equals(uniqueKeyField)) {
            // Check for duplicate IDs and store entity-scoped IDs for referential integrity check
            if (isOrderField) {
                // Check duplicate reference in set of field-scoped id:sequence (e.g., stop_sequence:12345:2)
                // This should not be scoped by key field because there may be conflicts (e.g., with trip_id="12345:2"
                listOfUniqueIds = referenceTracker.transitIdsWithSequence;
                uniqueId = String.join(":", field.name, keyValue, string);
            }
            // Add ID and check duplicate reference in entity-scoped IDs (e.g., stop_id:12345)
            boolean valueAlreadyExists = !listOfUniqueIds.add(uniqueId);
            if (valueAlreadyExists) {
                // If the value is a duplicate, add an error.
                NewGTFSError duplicateIdError = NewGTFSError.forLine(this, lineNumber, DUPLICATE_ID, uniqueId)
                        .setEntityId(keyValue);
                if (isOrderField) {
                    duplicateIdError.setSequence(string);
                }
                referenceTracker.errorStorage.storeError(duplicateIdError);
            }
        } else {
            // If the field is not the table unique key field, skip the duplicate ID check and simply add the ID to the
            // list of unique IDs.
            listOfUniqueIds.add(uniqueId);
        }
    }
}
