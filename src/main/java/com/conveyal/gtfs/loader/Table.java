package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.model.*;
import com.conveyal.gtfs.storage.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

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

    final String name;

    final Class<? extends Entity> entityClass;

    final boolean required;

    final Field[] fields;
/*
-- FeedInfo table is pulled outside per-feed schema.
-- Supply feed_id hint from outside.
-- CreateFeedsTable
create table feed_info (
    pkey number -- for schemaname
    -- required fields
    feed_publisher_name varchar,
    feed_publisher_url varchar,
    feed_lang varchar,
    -- optional fields
    feed_start_date date,
    feed_end_date date,
    feed_version varchar
    -- extension fields
    feed_id varchar,
    filename varchar,
)
 */
    public static final Table AGENCIES = new Table("agency", Agency.class, true,
            new StringField("agency_id",  OPTIONAL), // FIXME? only required if there are more than one
            new StringField("agency_name",  REQUIRED),
            new URLField("agency_url",  REQUIRED),
            new StringField("agency_timezone",  REQUIRED), // FIXME new type?
            new StringField("agency_lang", OPTIONAL), // FIXME new type?
            new StringField("agency_phone",  OPTIONAL),
            new URLField("agency_fare_url",  OPTIONAL),
            new StringField("agency_email",  OPTIONAL) // FIXME new type?
    );

    public static final Table ROUTES = new Table("routes", Route.class, true,
        new StringField("route_id",  REQUIRED),
        new StringField("agency_id",  OPTIONAL),
        new StringField("route_short_name",  OPTIONAL), // one of short or long must be provided
        new StringField("route_long_name",  OPTIONAL),
        new StringField("route_desc",  OPTIONAL),
        new IntegerField("route_type", REQUIRED, 999),
        new URLField("route_url",  OPTIONAL),
        new ColorField("route_color",  OPTIONAL), // really this is an int in hex notation
        new ColorField("route_text_color",  OPTIONAL)
    );

    public static final Table STOPS = new Table("stops", Stop.class, true,
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
    );

    public static final Table TRIPS = new Table("trips", Trip.class, true,
        new StringField("trip_id",  REQUIRED),
        new StringField("route_id",  REQUIRED),
        new StringField("service_id",  REQUIRED),
        new StringField("trip_headsign",  OPTIONAL),
        new StringField("trip_short_name",  OPTIONAL),
        new ShortField("direction_id", OPTIONAL, 1),
        new StringField("block_id",  OPTIONAL),
        new StringField("shape_id",  OPTIONAL),
        new ShortField("wheelchair_accessible", OPTIONAL, 2),
        new ShortField("bikes_allowed", OPTIONAL, 2)
    );

    public static final Table STOP_TIMES = new Table("stop_times", StopTime.class, true,
        new StringField("trip_id", REQUIRED),
        new IntegerField("stop_sequence", REQUIRED),
        new StringField("stop_id", REQUIRED),
        new TimeField("arrival_time", REQUIRED),
        new TimeField("departure_time", REQUIRED),
        new StringField("stop_headsign", OPTIONAL),
        new ShortField("pickup_type", OPTIONAL, 2),
        new ShortField("drop_off_type", OPTIONAL, 2),
        new DoubleField("shape_dist_traveled", OPTIONAL, 0, Double.POSITIVE_INFINITY),
        new ShortField("timepoint", OPTIONAL, 1),
        new IntegerField("fare_units_traveled", EXTENSION) // OpenOV NL extension
    );

    public static final Table SHAPES = new Table("shapes", ShapePoint.class, false,
        new StringField("shape_id", REQUIRED),
        new IntegerField("shape_pt_sequence", REQUIRED),
        new DoubleField("shape_pt_lat", REQUIRED, -80, 80),
        new DoubleField("shape_pt_lon", REQUIRED, -180, 180),
        new DoubleField("shape_dist_traveled", REQUIRED, 0, Double.POSITIVE_INFINITY)
    );

    public Table (String name, Class<? extends Entity> entityClass, boolean required, Field... fields) {
        this.name = name;
        this.entityClass = entityClass;
        this.required = required;
        this.fields = fields;
    }

    /**
     * Create an SQL table with all the fields specified by this table object,
     * plus an integer CSV line number field in the first position.
     */
    public void createSqlTable (Connection connection) {
        String fieldDeclarations = Arrays.stream(fields).map(Field::getSqlDeclaration).collect(Collectors.joining(", "));
        String dropSql = String.format("drop table if exists %s", name);
        // Adding the unlogged keyword gives about 12 percent speedup on loading, but is non-standard.
        String createSql = String.format("create table %s (csv_line integer, %s)", name, fieldDeclarations);
        try {
            Statement statement = connection.createStatement();
            statement.execute(dropSql);
            LOG.info(dropSql);
            statement.execute(createSql);
            LOG.info(createSql);
        } catch (Exception ex) {
            throw new StorageException(ex);
        }
    }

    /**
     * Create an SQL table that will insert a value into all the fields specified by this table object,
     * plus an integer CSV line number field in the first position.
     */
    public String generateInsertSql () {
        String questionMarks = String.join(", ", Collections.nCopies(fields.length, "?"));
        String joinedFieldNames = Arrays.stream(fields).map(f -> f.name).collect(Collectors.joining(", "));
        return String.format("insert into %s (csv_line, %s) values (?, %s)", name, joinedFieldNames, questionMarks);
    }

    /**
     * @param name a column name from the header of a CSV file
     * @return the Field object from this table with the given name. If there is no such field defined, create
     * a new Field object for this name.
     */
    public Field getFieldForName(String name) {
        // Linear search, assuming a small number of fields per table.
        for (Field field : fields) if (field.name.equals(name)) return field;
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

    public boolean isRequired () {
        return required;
    }

}
