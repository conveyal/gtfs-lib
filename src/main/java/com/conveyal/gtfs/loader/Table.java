package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.storage.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

import static com.conveyal.gtfs.loader.Requirement.*;


/**
 * This groups a table name with a description of the fields in the table.
 * It can be normative (expressing the specification for a CSV table in GTFS)
 * or descriptive (providing the schema of an RDBMS table).
 */
public class Table {

    private static final Logger LOG = LoggerFactory.getLogger(Table.class);

    String name;

    Field[] fields;

    public static final Table routes = new Table("routes",
        new StringField("route_id",  REQUIRED),
        new StringField("agency_id",  OPTIONAL),
        new StringField("route_short_name",  OPTIONAL), // one of short or long must be provided
        new StringField("route_long_name",  OPTIONAL),
        new StringField("route_desc",  OPTIONAL),
        new IntegerField("route_type", REQUIRED, 999),
        new URLField("route_url",  OPTIONAL),
        new StringField("route_color",  OPTIONAL), // really this is an int in hex notation
        new StringField("route_text_color",  OPTIONAL)
    );

    public static final Table stops = new Table("stops",
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

    public static final Table trips = new Table("trips",
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

    public static final Table stop_times = new Table("stop_times",
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

    public static final Table shapes = new Table("shapes",
        new StringField("shape_id", REQUIRED),
        new IntegerField("shape_pt_sequence", REQUIRED),
        new DoubleField("shape_pt_lat", REQUIRED, -80, 80),
        new DoubleField("shape_pt_lon", REQUIRED, -180, 180),
        new DoubleField("shape_dist_traveled", REQUIRED, 0, Double.POSITIVE_INFINITY)
    );

    public Table (String name, Field... fields) {
        this.name = name;
        this.fields = fields;
    }

    public void createSqlTable (Connection connection) {
        String fieldDeclarations = Arrays.stream(fields).map(Field::getSqlDeclaration).collect(Collectors.joining(", "));
        // Adding the unlogged keyword gives about 12 percent speedup, but is non-standard.
        String createSql = String.format("create table %s (%s)", name, fieldDeclarations);
        try {
            Statement statement = connection.createStatement();
            statement.execute("drop table if exists " + this.name);
            statement.execute(createSql);
            LOG.info(createSql);
        } catch (Exception ex) {
            throw new StorageException(ex);
        }
    }

    public String generateInsertSql () {
        String questionMarks = String.join(",", Collections.nCopies(fields.length, "?"));
        String joinedFieldNames = Arrays.stream(fields).map(f -> f.name).collect(Collectors.joining(", "));
        return String.format("insert into %s (%s) values (%s)", name, joinedFieldNames, questionMarks);
    }

    public Field getFieldForHeader (String fieldName) {
        for (Field field : fields) if (field.name.equals(fieldName)) return field;
        LOG.warn("Unrecognized header {}. Treating it as a proprietary string field.", fieldName);
        return new StringField(fieldName, UNKNOWN);
    }

}
