package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.model.*;
import com.conveyal.gtfs.model.Calendar;
import com.conveyal.gtfs.storage.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

import static com.conveyal.gtfs.error.NewGTFSErrorType.MISSING_FIELD;
import static com.conveyal.gtfs.loader.Requirement.*;


/**
 * This groups a table name with a description of the fields in the table.
 * It can be normative (expressing the specification for a CSV table in GTFS)
 * or descriptive (providing the schema of an RDBMS table).
 *
 * TODO associate Table with EntityPopulator (combine read and write sides)
 */
public class Table {

    private static final Logger LOG = LoggerFactory.getLogger(Table.class);
    // Represents null in Postgres text format
    private static final String POSTGRES_NULL_TEXT = "\\N";

    public final String name;

    final Class<? extends Entity> entityClass;

    final Requirement required;

    public final Field[] fields;
    /** Determines whether cascading delete is restricted. Defaults to false (i.e., cascade delete is allowed) */
    private boolean deleteRestricted = false;

    public Table (String name, Class<? extends Entity> entityClass, Requirement required, Field... fields) {
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
        new URLField("agency_fare_url",  OPTIONAL),
        new StringField("agency_email",  OPTIONAL) // FIXME new field type for emails?
    ).restrictDelete();

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
        new DateField("end_date", REQUIRED)
    ).restrictDelete();

    public static final Table CALENDAR_DATES = new Table("calendar_dates", CalendarDate.class, OPTIONAL,
        new StringField("service_id", REQUIRED),
        new IntegerField("date", REQUIRED),
        new IntegerField("exception_type", REQUIRED, 1, 2)
    );

    public static final Table FARE_ATTRIBUTES = new Table("fare_attributes", FareAttribute.class, OPTIONAL,
        new StringField("fare_id", REQUIRED),
        new DoubleField("price", REQUIRED, 0.0, Double.MAX_VALUE),
        new CurrencyField("currency_type", REQUIRED),
        new ShortField("payment_method", REQUIRED, 1),
        new ShortField("transfers", REQUIRED, 2),
        new IntegerField("transfer_duration", OPTIONAL)
    );


    public static final Table FEED_INFO = new Table("feed_info", FeedInfo.class, OPTIONAL,
        new StringField("feed_publisher_name", REQUIRED),
        new StringField("feed_publisher_url", REQUIRED),
        new LanguageField("feed_lang", REQUIRED),
        new DateField("feed_start_date", OPTIONAL),
        new DateField("feed_end_date", OPTIONAL),
        new StringField("feed_version", OPTIONAL)

    );


    public static final Table ROUTES = new Table("routes", Route.class, REQUIRED,
        new StringField("route_id",  REQUIRED),
        new StringField("agency_id",  OPTIONAL, AGENCY),
        new StringField("route_short_name",  OPTIONAL), // one of short or long must be provided
        new StringField("route_long_name",  OPTIONAL),
        new StringField("route_desc",  OPTIONAL),
        new IntegerField("route_type", REQUIRED, 999),
        new URLField("route_url",  OPTIONAL),
        new ColorField("route_color",  OPTIONAL), // really this is an int in hex notation
        new ColorField("route_text_color",  OPTIONAL)
    );

    public static final Table FARE_RULES = new Table("fare_rules", FareRule.class, OPTIONAL,
            new StringField("fare_id", REQUIRED, FARE_ATTRIBUTES),
            new StringField("route_id", OPTIONAL, ROUTES),
            // FIXME: referential integrity check for zone_id for below three fields?
            new StringField("origin_id", OPTIONAL),
            new StringField("destination_id", OPTIONAL),
            new StringField("contains_id", OPTIONAL)
    );

    public static final Table PATTERNS = new Table("patterns", Pattern.class, OPTIONAL,
            new StringField("pattern_id", REQUIRED),
            new StringField("route_id", REQUIRED, ROUTES),
            new StringField("description", OPTIONAL)
    );

    public static final Table SHAPES = new Table("shapes", ShapePoint.class, OPTIONAL,
            new StringField("shape_id", REQUIRED),
            new IntegerField("shape_pt_sequence", REQUIRED),
            new DoubleField("shape_pt_lat", REQUIRED, -80, 80),
            new DoubleField("shape_pt_lon", REQUIRED, -180, 180),
            new DoubleField("shape_dist_traveled", REQUIRED, 0, Double.POSITIVE_INFINITY)
    );

    public static final Table STOPS = new Table("stops", Stop.class, REQUIRED,
        new StringField("stop_id",  REQUIRED),
        new StringField("stop_code",  OPTIONAL),
        new StringField("stop_name",  REQUIRED),
        new StringField("stop_desc",  OPTIONAL),
        new DoubleField("stop_lat", REQUIRED, -80, 80),
        new DoubleField("stop_lon", REQUIRED, -180, 180),
        new StringField("zone_id",  OPTIONAL),
        new URLField("stop_url",  OPTIONAL),
        new ShortField("location_type", OPTIONAL, 2),
        new StringField("parent_station",  OPTIONAL),
        new StringField("stop_timezone",  OPTIONAL),
        new ShortField("wheelchair_boarding", OPTIONAL, 1)
    ).restrictDelete();

    public static final Table PATTERN_STOP = new Table("pattern_stops", PatternStop.class, OPTIONAL,
            new StringField("pattern_id", REQUIRED, PATTERNS),
            // FIXME: Do we need an index on stop_id
            new StringField("stop_id", REQUIRED, STOPS),
            new IntegerField("stop_sequence", REQUIRED)
    );

    public static final Table TRANSFERS = new Table("transfers", Transfer.class, OPTIONAL,
            // FIXME: Do we need an index on from_ and to_stop_id
            new StringField("from_stop_id", REQUIRED, STOPS),
            new StringField("to_stop_id", REQUIRED, STOPS),
            new StringField("transfer_type", REQUIRED),
            new StringField("min_transfer_time", OPTIONAL)
    );

    public static final Table TRIPS = new Table("trips", Trip.class, REQUIRED,
        new StringField("trip_id",  REQUIRED),
        new StringField("route_id",  REQUIRED, ROUTES).indexThisColumn(),
        // FIXME: Should this also optionally reference CALENDAR_DATES?
        // FIXME: Do we need an index on service_id
        new StringField("service_id",  REQUIRED, CALENDAR),
        new StringField("trip_headsign",  OPTIONAL),
        new StringField("trip_short_name",  OPTIONAL),
        new ShortField("direction_id", OPTIONAL, 1),
        new StringField("block_id",  OPTIONAL),
        new StringField("shape_id",  OPTIONAL, SHAPES),
        new ShortField("wheelchair_accessible", OPTIONAL, 2),
        new ShortField("bikes_allowed", OPTIONAL, 2)
    );

    // Must come after TRIPS and STOPS table to which it has references
    public static final Table STOP_TIMES = new Table("stop_times", StopTime.class, REQUIRED,
            new StringField("trip_id", REQUIRED, TRIPS),
            new IntegerField("stop_sequence", REQUIRED),
            // FIXME: Do we need an index on stop_id
            new StringField("stop_id", REQUIRED, STOPS),
//                    .indexThisColumn(),
            // TODO verify that we have a special check for arrival and departure times first and last stop_time in a trip, which are reqiured
            new TimeField("arrival_time", OPTIONAL),
            new TimeField("departure_time", OPTIONAL),
            new StringField("stop_headsign", OPTIONAL),
            new ShortField("pickup_type", OPTIONAL, 2),
            new ShortField("drop_off_type", OPTIONAL, 2),
            new DoubleField("shape_dist_traveled", OPTIONAL, 0, Double.POSITIVE_INFINITY),
            new ShortField("timepoint", OPTIONAL, 1),
            new IntegerField("fare_units_traveled", EXTENSION) // OpenOV NL extension
    );

    // Must come after TRIPS table to which it has a reference
    public static final Table FREQUENCIES = new Table("frequencies", Frequency.class, OPTIONAL,
            new StringField("trip_id", REQUIRED, TRIPS),
            new TimeField("start_time", REQUIRED),
            new TimeField("end_time", REQUIRED),
            new IntegerField("headway_secs", REQUIRED, 20, 60*60*2),
            new IntegerField("exact_times", OPTIONAL, 1)
    );

    public static final Table[] tablesInOrder = {
            AGENCY,
            CALENDAR,
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
        this.deleteRestricted = true;
        return this;
    }

    public boolean isDeleteRestricted() {
        return deleteRestricted;
    }

    /**
     * Create an SQL table with all the fields specified by this table object,
     * plus an integer CSV line number field in the first position.
     */
    public void createSqlTable (Connection connection) {
        String fieldDeclarations = Arrays.stream(fields).map(Field::getSqlDeclaration).collect(Collectors.joining(", "));
        String dropSql = String.format("drop table if exists %s", name);
        // Adding the unlogged keyword gives about 12 percent speedup on loading, but is non-standard.
        String createSql = String.format("create table %s (id bigint not null, %s)", name, fieldDeclarations);
        try {
            Statement statement = connection.createStatement();
            LOG.info(dropSql);
            statement.execute(dropSql);
            LOG.info(createSql);
            statement.execute(createSql);
        } catch (Exception ex) {
            throw new StorageException(ex);
        }
    }

    /**
     * Create an SQL table that will insert a value into all the fields specified by this table object,
     * plus an integer CSV line number field in the first position.
     */
    public String generateInsertSql () {
        return generateInsertSql(null);
    }


    public String generateInsertSql (String namespace) {
        String tableName = namespace == null ? name : String.join(".", namespace, name);
        String questionMarks = String.join(", ", Collections.nCopies(fields.length, "?"));
        String joinedFieldNames = Arrays.stream(fields).map(f -> f.name).collect(Collectors.joining(", "));
        return String.format("insert into %s (id, %s) values (?, %s)", tableName, joinedFieldNames, questionMarks);
    }

    /**
     * Set value for a field either as a prepared statement parameter or (if using postgres text-loading) in the
     * transformed strings array provided. This also handles the case where the string is empty (i.e., field is null)
     * and when an exception is encountered while setting the field value (usually due to a bad data type), in which case
     * the field is set to null.
     */
    public void setValueForField(PreparedStatement statement, int fieldIndex, int lineNumber, Field field, String string, SQLErrorStorage errorStorage, boolean postgresText, String[] transformedStrings) {
        if (string.isEmpty()) {
            // CSV reader always returns empty strings, not nulls
            if (field.isRequired() && errorStorage != null) {
                errorStorage.storeError(NewGTFSError.forLine(this, lineNumber, MISSING_FIELD, field.name));
            }
            // Set field to null if string is empty
            setFieldToNull(postgresText, transformedStrings, statement, fieldIndex, field);
        } else {
            // Micro-benchmarks show it's only 4-5% faster to call typed parameter setter methods
            // rather than setObject with a type code. I think some databases don't have setObject though.
            // The Field objects throw exceptions to avoid passing the line number, table name etc. into them.
            try {
                // FIXME we need to set the transformed string element even when an error occurs.
                // This means the validation and insertion step need to happen separately.
                // or the errors should not be signaled with exceptions.
                // Also, we should probably not be converting any GTFS field values.
                // We should be saving it as-is in the database and converting upon load into our model objects.
                if (postgresText) transformedStrings[fieldIndex + 1] = field.validateAndConvert(string);
                else field.setParameter(statement, fieldIndex + 2, string);
            } catch (StorageException ex) {
                // FIXME many exceptions don't have an error type
                if (errorStorage != null) {
                    errorStorage.storeError(NewGTFSError.forLine(this, lineNumber, ex.errorType, ex.badValue));
                }
                // Set transformedStrings or prepared statement param to null
                setFieldToNull(postgresText, transformedStrings, statement, fieldIndex, field);
            }
        }
    }

    /**
     * Sets field to null in statement or string array depending on whether postgres is being used.
     */
    private static void setFieldToNull(boolean postgresText, String[] transformedStrings, PreparedStatement statement, int fieldIndex, Field field) {
        if (postgresText) transformedStrings[fieldIndex + 1] = POSTGRES_NULL_TEXT;
            // Adjust parameter index by two: indexes are one-based and the first one is the CSV line number.
        else try {
            statement.setNull(fieldIndex + 2, field.getSqlType().getVendorTypeNumber());
        } catch (SQLException e) {
            e.printStackTrace();
            // FIXME: store error here? It appears that an exception should only be thrown if the type value is invalid,
            // the connection is closed, or the index is out of bounds. So storing an error may be unnecessary.
        }
    }

    /**
     * Generate select all SQL string.
     */
    public String generateSelectSql (String namespace) {
        return String.format("select * from %s", String.join(".", namespace, name));
    }

    /**
     * Generate delete SQL string.
     */
    public String generateDeleteSql (String namespace) {
        return String.format("delete from %s where id = ?", String.join(".", namespace, name));
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

    public String getKeyFieldName () {
        return fields[0].name;
    }

    public String getOrderFieldName () {
        String name = fields[1].name;
        if (name.contains("_sequence")) return name;
        else return null;
    }

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
     * Create indexes for table using shouldBeIndexed(), key field, and/or sequence field.
     * FIXME: add foreign reference indexes?
     */
    public void createIndexes(Connection connection) throws SQLException {
        LOG.info("Indexing...");
        // We determine which columns should be indexed based on field order in the GTFS spec model table.
        // Not sure that's a good idea, this could use some abstraction. TODO getIndexColumns() on each table.
        String indexColumns = getIndexFields();
        // TODO verify referential integrity and uniqueness of keys
        // TODO create primary key and fall back on plain index (consider not null & unique constraints)
        // TODO use line number as primary key
        // Note: SQLITE requires specifying a name for indexes.
        String indexName = String.join("_", name.replace(".", "_"), "idx");
        String indexSql = String.format("create index %s on %s (%s)", indexName, name, indexColumns);
        //String indexSql = String.format("alter table %s add primary key (%s)", table.name, indexColumns);
        LOG.info(indexSql);
        connection.createStatement().execute(indexSql);
        // TODO add foreign key constraints, and recover recording errors as needed.

        // More indexing
        // TODO integrate with the above indexing code, iterating over a List<String> of index column expressions
        for (Field field : fields) {
            if (field.shouldBeIndexed()) {
                Statement statement = connection.createStatement();
                String fieldIndex = String.join("_", name.replace(".", "_"), field.name, "idx");
                String sql = String.format("create index %s on %s (%s)", fieldIndex, name, field.name);
                LOG.info(sql);
                statement.execute(sql);
            }
        }
    }

    /**
     * Creates a SQL table from the table to clone. This uses the SQL syntax "create table x as y" not only copies the
     * table structure, but also the data from the original table. Creating table indexes is not handled by this method.
     * @return
     */
    public boolean createSqlTableFrom(Connection connection, String tableToClone, boolean addPrimaryKey) {
        String dropSql = String.format("drop table if exists %s", name);
        // Adding the unlogged keyword gives about 12 percent speedup on loading, but is non-standard.
        // FIXME: Which create table operation is more efficient?
        String createTableAsSql = String.format("create table %s as table %s", name, tableToClone);
//        String createTableLikeSql = String.format("create table %s (like %s including indexes)", name, tableToClone);
//        String insertAllSql = String.format("insert into %s select * from %s", name, tableToClone);
        try {
            Statement statement = connection.createStatement();
            LOG.info(dropSql);
            statement.execute(dropSql);
            LOG.info(createTableAsSql);
            statement.execute(createTableAsSql);
//            LOG.info(createTableLikeSql);
//            statement.execute(createTableLikeSql);
//            LOG.info(insertAllSql);
//            statement.execute(insertAllSql);


            // Make id column serial and set the next value based on the current max value. This code is derived from
            // https://stackoverflow.com/a/9490532/915811
            // FIXME: For now we skip the Pattern and PatternStop id creation because neither has an ID field.
            if (!this.entityClass.equals(Pattern.class) && !this.entityClass.equals(PatternStop.class)) {
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
                // FIXME: Is there a need to add primary key constraint here?
                if (addPrimaryKey) {
                    // Add primary key to ID column for any tables that require it.
                    String addPrimaryKeySql = String.format("ALTER TABLE %s ADD PRIMARY KEY (id)", name);
                    LOG.info(addPrimaryKeySql);
                    statement.execute(addPrimaryKeySql);
                }
            }

            return true;
        } catch (SQLException ex) {
            LOG.error("Error cloning table {}: {}", name, ex.getSQLState());
            LOG.error("details: ", ex);
            try {
                connection.rollback();
                // FIXME: Try to create table without cloning?
                createSqlTable(connection);
                return true;
            } catch (SQLException e) {
                e.printStackTrace();
                return false;
            }
        }
    }
}
