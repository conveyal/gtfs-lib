package com.conveyal.gtfs.graphql;

import com.conveyal.gtfs.graphql.fetchers.ErrorCountFetcher;
import com.conveyal.gtfs.graphql.fetchers.FeedFetcher;
import com.conveyal.gtfs.graphql.fetchers.JDBCFetcher;
import com.conveyal.gtfs.graphql.fetchers.MapFetcher;
import com.conveyal.gtfs.graphql.fetchers.NestedJDBCFetcher;
import com.conveyal.gtfs.graphql.fetchers.RowCountFetcher;
import com.conveyal.gtfs.graphql.fetchers.SQLColumnFetcher;
import com.conveyal.gtfs.graphql.fetchers.SourceObjectFetcher;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLTypeReference;

import static com.conveyal.gtfs.graphql.GraphQLUtil.intArg;
import static com.conveyal.gtfs.graphql.GraphQLUtil.intt;
import static com.conveyal.gtfs.graphql.GraphQLUtil.multiStringArg;
import static com.conveyal.gtfs.graphql.GraphQLUtil.string;
import static com.conveyal.gtfs.graphql.GraphQLUtil.stringArg;
import static com.conveyal.gtfs.graphql.fetchers.JDBCFetcher.*;
import static graphql.Scalars.GraphQLFloat;
import static graphql.Scalars.GraphQLInt;
import static graphql.Scalars.GraphQLString;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;
import static graphql.schema.GraphQLObjectType.newObject;

/**
 * This defines the types for our GraphQL API, and wires them up to functions that can fetch data from JDBC databases.
 */
public class GraphQLGtfsSchema {

    // The order here is critical. Each new type that's defined can refer to other types directly by object
    // reference or by name. Names can only be used for types that are already reachable recursively by
    // reference from the top of the schema. So you want as many direct references as you can.
    // It really seems like all this should be done automatically, maybe we should be using a text schema
    // instead of code.
    // I do wonder whether these should all be statically initialized. Doing this in a non-static context
    // in one big block with local variables, the dependencies would be checked by compiler.
    // The order:
    // Instantiate starting with leaf nodes (reverse topological sort of the dependency graph).
    // All forward references must use names and GraphQLTypeReference.
    // Additionally the tree will be explored once top-down following explicit object references, and only
    // objects reached that way will be available by name reference.
    // Another way to accomplish this would be to use name references in every definition except the top level,
    // and make a dummy declaration that will call them all to be pulled in by reference at once.


    // The old types are defined in separate class files. I'm defining new ones here.

    // by using static fields to hold these types, backward references are enforced. a few forward references are inserted explicitly.

    // Represents rows from agency.txt
    public static final GraphQLObjectType agencyType = newObject().name("agency")
            .description("A GTFS agency object")
            .field(MapFetcher.field("id", GraphQLInt))
            .field(MapFetcher.field("agency_id"))
            .field(MapFetcher.field("agency_name"))
            .field(MapFetcher.field("agency_url"))
            .field(MapFetcher.field("agency_branding_url"))
            .field(MapFetcher.field("agency_desc"))
            .field(MapFetcher.field("agency_fare_url"))
            .field(MapFetcher.field("agency_timezone"))
            .build();

    // Represents rows from calendar.txt
    public static final GraphQLObjectType calendarType = newObject()
            .name("calendar")
            .field(MapFetcher.field("id", GraphQLInt))
            .field(MapFetcher.field("service_id"))
            .field(MapFetcher.field("monday", GraphQLInt))
            .field(MapFetcher.field("tuesday", GraphQLInt))
            .field(MapFetcher.field("wednesday", GraphQLInt))
            .field(MapFetcher.field("thursday", GraphQLInt))
            .field(MapFetcher.field("friday", GraphQLInt))
            .field(MapFetcher.field("saturday", GraphQLInt))
            .field(MapFetcher.field("sunday", GraphQLInt))
            .field(MapFetcher.field("start_date"))
            .field(MapFetcher.field("end_date"))
            .build();


    // Represents rows from fare_rules.txt
    public static final GraphQLObjectType fareRuleType = newObject().name("fareRule")
            .description("A GTFS agency object")
            .field(MapFetcher.field("id", GraphQLInt))
            .field(MapFetcher.field("fare_id"))
            .field(MapFetcher.field("route_id"))
            .field(MapFetcher.field("origin_id"))
            .field(MapFetcher.field("destination_id"))
            .field(MapFetcher.field("contains_id"))
            .build();

    // Represents rows from fare_attributes.txt
    public static final GraphQLObjectType fareType = newObject().name("fare_attributes")
            .description("A GTFS agency object")
            .field(MapFetcher.field("id", GraphQLInt))
            .field(MapFetcher.field("fare_id"))
            .field(MapFetcher.field("price", GraphQLFloat))
            .field(MapFetcher.field("currency_type"))
            .field(MapFetcher.field("payment_method", GraphQLInt))
            .field(MapFetcher.field("transfers", GraphQLInt))
            .field(MapFetcher.field("transfer_duration", GraphQLInt))
            .field(newFieldDefinition()
                    .name("fare_rules")
                    .type(new GraphQLList(fareRuleType))
                    .dataFetcher(new JDBCFetcher("fare_rules", "fare_id"))
                    .build()
            )
            .build();

    // Represents feed_info.txt
    public static final GraphQLObjectType feedInfoType = newObject().name("feed_info")
            .description("A GTFS feed_info object")
            .field(MapFetcher.field("id", GraphQLInt))
            .field(MapFetcher.field("feed_publisher_name"))
            .field(MapFetcher.field("feed_publisher_url"))
            .field(MapFetcher.field("feed_lang"))
            .field(MapFetcher.field("feed_start_date"))
            .field(MapFetcher.field("feed_end_date"))
            .field(MapFetcher.field("feed_version"))
            .build();

    // Represents rows from shapes.txt
    public static final GraphQLObjectType shapePointType = newObject().name("shapePoint")
            .field(MapFetcher.field("shape_id"))
            .field(MapFetcher.field("shape_dist_traveled", GraphQLFloat))
            .field(MapFetcher.field("shape_pt_lat", GraphQLFloat))
            .field(MapFetcher.field("shape_pt_lon", GraphQLFloat))
            .field(MapFetcher.field("shape_pt_sequence", GraphQLInt))
            .field(MapFetcher.field("point_type", GraphQLInt))
            .build();

    // Represents rows from trips.txt
    public static final GraphQLObjectType tripType = newObject()
            .name("trip")
            .field(MapFetcher.field("id", GraphQLInt))
            .field(MapFetcher.field("trip_id"))
            .field(MapFetcher.field("trip_headsign"))
            .field(MapFetcher.field("trip_short_name"))
            .field(MapFetcher.field("block_id"))
            .field(MapFetcher.field("direction_id", GraphQLInt))
            .field(MapFetcher.field("route_id"))
            .field(MapFetcher.field("service_id"))
            .field(MapFetcher.field("pattern_id"))
            .field(newFieldDefinition()
                    .name("stop_times")
                    // forward reference to the as yet undefined stopTimeType (must be defined after tripType)
                    .type(new GraphQLList(new GraphQLTypeReference("stopTime")))
                    .dataFetcher(new JDBCFetcher("stop_times", "trip_id", "stop_sequence"))
                    .build()
            )
            // TODO should this be included in the query?
            .field(newFieldDefinition()
                    .name("shape")
                    .type(new GraphQLList(shapePointType))
                    .dataFetcher(new JDBCFetcher("shapes", "shape_id"))
                    .build())
//            // some pseudo-fields to reduce the amount of data that has to be fetched over GraphQL to summarize
//            .field(newFieldDefinition()
//                    .name("start_time")
//                    .type(GraphQLInt)
//                    .dataFetcher(TripDataFetcher::getStartTime)
//                    .build()
//            )
//            .field(newFieldDefinition()
//                    .name("duration")
//                    .type(GraphQLInt)
//                    .dataFetcher(TripDataFetcher::getDuration)
//                    .build()
//            )
            .build();


    // Represents rows from stop_times.txt
    public static final GraphQLObjectType stopTimeType = newObject().name("stopTime")
            .field(MapFetcher.field("trip_id"))
            .field(MapFetcher.field("stop_id"))
            .field(MapFetcher.field("stop_sequence", GraphQLInt))
            .field(MapFetcher.field("arrival_time", GraphQLInt))
            .field(MapFetcher.field("departure_time", GraphQLInt))
            .field(MapFetcher.field("stop_headsign"))
            .field(MapFetcher.field("shape_dist_traveled", GraphQLFloat))
            .build();

    // Represents rows from routes.txt
    public static final GraphQLObjectType routeType = newObject().name("route")
            .description("A line from a GTFS routes.txt table")
            .field(MapFetcher.field("id", GraphQLInt))
            .field(MapFetcher.field("agency_id"))
            .field(MapFetcher.field("route_id"))
            .field(MapFetcher.field("route_short_name"))
            .field(MapFetcher.field("route_long_name"))
            .field(MapFetcher.field("route_desc"))
            .field(MapFetcher.field("route_url"))
            .field(MapFetcher.field("route_branding_url"))
            .field(MapFetcher.field("wheelchair_boarding"))
            // TODO route_type as enum or int
            .field(MapFetcher.field("route_type"))
            .field(MapFetcher.field("route_color"))
            .field(MapFetcher.field("route_text_color"))
            .field(RowCountFetcher.field("trip_count", "trips", "route_id"))
            .field(RowCountFetcher.field("pattern_count", "patterns", "route_id"))
            .field(newFieldDefinition()
                    .type(new GraphQLList(tripType))
                    .name("trips")
                    .argument(multiStringArg("trip_id"))
                    .dataFetcher(new JDBCFetcher("trips", "route_id"))
                    .build()
            )
            .field(newFieldDefinition()
                    .type(new GraphQLList(new GraphQLTypeReference("pattern")))
                    .name("patterns")
                    .argument(multiStringArg("pattern_id"))
                    .dataFetcher(new JDBCFetcher("patterns", "route_id"))
                    .build()
            )
            .field(RowCountFetcher.field("count", "routes"))
            .build();

    // Represents rows from stops.txt
    // Contains a reference to stopTimeType and routeType
    public static final GraphQLObjectType stopType = newObject().name("stop")
            .description("A GTFS stop object")
            .field(MapFetcher.field("id", GraphQLInt))
            .field(MapFetcher.field("stop_id"))
            .field(MapFetcher.field("stop_name"))
            .field(MapFetcher.field("stop_code"))
            .field(MapFetcher.field("stop_desc"))
            .field(MapFetcher.field("stop_lon", GraphQLFloat))
            .field(MapFetcher.field("stop_lat", GraphQLFloat))
            .field(MapFetcher.field("zone_id"))
            .field(MapFetcher.field("stop_url"))
            .field(MapFetcher.field("stop_timezone"))
            .field(MapFetcher.field("parent_station"))
            .field(MapFetcher.field("location_type", GraphQLInt))
            .field(MapFetcher.field("wheelchair_boarding", GraphQLInt))
            .field(RowCountFetcher.field("stop_time_count", "stop_times", "stop_id"))
            .field(newFieldDefinition()
                    .name("routes")
                    // Field type should be equivalent to the final JDBCFetcher table type.
                    .type(new GraphQLList(routeType))
                    .argument(stringArg("namespace"))
                    .dataFetcher(new NestedJDBCFetcher(
                            new JDBCFetcher("pattern_stops", "stop_id"),
                            new JDBCFetcher("patterns", "pattern_id"),
                            new JDBCFetcher("routes", "route_id")))
                    .build())
//            .field(newFieldDefinition()
//                    .name("stop_times")
//                    .description("The list of stop_times for a stop")
//                    .type(new GraphQLList(GraphQLGtfsSchema.stopTimeType))
//                    .argument(stringArg("date"))
//                    .argument(longArg("from"))
//                    .argument(longArg("to"))
//                    .dataFetcher(StopTimeFetcher::fromStop)
//                    .build()
//            )
//            .field(newFieldDefinition()
//                    .name("routes")
//                    .description("The list of routes that serve a stop")
//                    .type(new GraphQLList(GraphQLGtfsSchema.routeType))
//                    .argument(multiStringArg("route_id"))
//                    .dataFetcher(RouteFetcher::fromStop)
//                    .build()
//            )
            .build();

    /**
     * Represents each stop in a list of stops within a pattern.
     * We could return just a list of StopIDs within the pattern (a JSON array of strings) but
     * that structure would prevent us from joining tables and returning additional stop details
     * like lat and lon, or pickup and dropoff types if we add those to the pattern signature.
     */
    public static final GraphQLObjectType patternStopType = newObject().name("patternStop")
            .field(MapFetcher.field("id", GraphQLInt))
            .field(MapFetcher.field("pattern_id"))
            .field(MapFetcher.field("stop_id"))
            .field(MapFetcher.field("default_travel_time", GraphQLInt))
            .field(MapFetcher.field("default_dwell_time", GraphQLInt))
            .field(MapFetcher.field("shape_dist_traveled", GraphQLFloat))
            .field(MapFetcher.field("drop_off_type", GraphQLInt))
            .field(MapFetcher.field("pickup_type", GraphQLInt))
            .field(MapFetcher.field("timepoint", GraphQLInt))
            .field(newFieldDefinition()
                    .type(stopType)
                    .name("stop")
                    .dataFetcher(new JDBCFetcher("stops", "stop_id"))
                    .build()
            )
            .field(MapFetcher.field("stop_sequence", GraphQLInt))
            .build();

    /**
     * The GraphQL API type representing entries in the table of errors encountered while loading or validating a feed.
     */
    public static GraphQLObjectType validationErrorType = newObject().name("validationError")
            .description("An error detected when loading or validating a feed.")
            .field(MapFetcher.field("error_id", GraphQLInt))
            .field(MapFetcher.field("error_type"))
            .field(MapFetcher.field("entity_type"))
            // FIXME: change to id?
            .field(MapFetcher.field("line_number", GraphQLInt))
            .field(MapFetcher.field("entity_id"))
            .field(MapFetcher.field("entity_sequence", GraphQLInt))
            .field(MapFetcher.field("bad_value"))
            .build();

    /**
     * The GraphQL API type representing counts of rows in the various GTFS tables.
     * The context here for fetching subfields is the feedType. A special dataFetcher is used to pass that identical
     * context down.
     */
    public static GraphQLObjectType rowCountsType = newObject().name("rowCounts")
            .description("Counts of rows in the various GTFS tables.")
            .field(RowCountFetcher.field("stops"))
            .field(RowCountFetcher.field("trips"))
            .field(RowCountFetcher.field("routes"))
            .field(RowCountFetcher.field("stop_times"))
            .field(RowCountFetcher.field("agency"))
            .field(RowCountFetcher.field("calendar"))
            .field(RowCountFetcher.field("calendar_dates"))
            .field(RowCountFetcher.field("errors"))
            .build();

    /**
     * GraphQL does not have a type for arbitrary maps (String -> X). Such maps must be expressed as a list of
     * key-value pairs. This is probably intended to protect us from ourselves (sending untyped data) but it just
     * leads to silly workarounds like this.
     */
    public static GraphQLObjectType errorCountType = newObject().name("errorCount")
            .description("Quantity of validation errors of a specific type.")
            .field(string("type"))
            .field(intt("count"))
            .field(string("message"))
            .build();

    /**
     * The GraphQL API type representing a unique sequence of stops on a route. This is used to group trips together.
     */
    public static final GraphQLObjectType patternType = newObject().name("pattern")
            .description("A sequence of stops that characterizes a set of trips on a single route.")
            .field(MapFetcher.field("id", GraphQLInt))
            .field(MapFetcher.field("pattern_id"))
            .field(MapFetcher.field("shape_id"))
            .field(MapFetcher.field("route_id"))
            .field(MapFetcher.field("name"))
            .field(newFieldDefinition()
                    .name("shape")
                    .type(new GraphQLList(shapePointType))
                    .argument(intArg(LIMIT_ARG))
                    .dataFetcher(new JDBCFetcher("shapes",
                            "shape_id",
                            "shape_pt_sequence"))
                    .build())
            .field(RowCountFetcher.field("trip_count", "trips", "pattern_id"))
            .field(newFieldDefinition()
                .name("stops")
                .type(new GraphQLList(patternStopType))
                .dataFetcher(new JDBCFetcher("pattern_stops",
                        "pattern_id",
                        "stop_sequence"))
                .build())
            .field(newFieldDefinition()
                .name("trips")
                .type(new GraphQLList(tripType))
                .argument(multiStringArg("service_id"))
                .dataFetcher(new JDBCFetcher("trips", "pattern_id"))
                .build())
            .build();

    /**
     * Durations that a service runs on each mode of transport (route_type).
     */
    public static final GraphQLObjectType serviceDurationType = newObject().name("serviceDuration")
            .field(MapFetcher.field("route_type", GraphQLInt))
            .field(MapFetcher.field("duration_seconds", GraphQLInt))
            .build();

    /**
     * The GraphQL API type representing a service (a service_id attached to trips to say they run on certain days).
     */
    public static GraphQLObjectType serviceType = newObject().name("service")
            .description("A group of trips that all run together on certain days.")
            .field(MapFetcher.field("service_id"))
            .field(MapFetcher.field("n_days_active"))
            .field(MapFetcher.field("duration_seconds"))
            .field(newFieldDefinition()
                    .name("dates")
                    .type(new GraphQLList(GraphQLString))
                    .dataFetcher(new SQLColumnFetcher<String>("service_dates", "service_id", "service_date"))
                    .build())
            .field(newFieldDefinition()
                    .name("trips")
                    .type(new GraphQLList(tripType))
                    .dataFetcher(new JDBCFetcher("trips", "service_id"))
                    .build())
            .field(newFieldDefinition()
                    .name("durations")
                    .type(new GraphQLList(serviceDurationType))
                    .dataFetcher(new JDBCFetcher("service_durations", "service_id"))
                    .build())
            .build();

    /**
     * The GraphQL API type representing entries in the top-level table listing all the feeds imported into a gtfs-api
     * database, and with sub-fields for each table of GTFS entities within a single feed.
     */
    public static final GraphQLObjectType feedType = newObject().name("feedVersion")
            // First, the fields present in the top level table.
            .field(MapFetcher.field("namespace"))
            .field(MapFetcher.field("feed_id"))
            .field(MapFetcher.field("feed_version"))
            .field(MapFetcher.field("filename"))
            .field(MapFetcher.field("md5"))
            .field(MapFetcher.field("sha1"))
            .field(MapFetcher.field("snapshot_of"))
            // A field containing row counts for every table.
            .field(newFieldDefinition()
                    .name("row_counts")
                    .type(rowCountsType)
                    .dataFetcher(new SourceObjectFetcher())
                    .build())
            // A field containing counts for each type of error independently.
            .field(newFieldDefinition()
                    .name("error_counts")
                    .type(new GraphQLList(errorCountType))
                    .dataFetcher(new ErrorCountFetcher())
                    .build())
            // A field for the errors themselves.
            .field(newFieldDefinition()
                    .name("errors")
                    .type(new GraphQLList(validationErrorType))
                    .argument(stringArg("namespace"))
                    .argument(multiStringArg("error_type"))
                    .argument(intArg(LIMIT_ARG))
                    .argument(intArg(OFFSET_ARG))
                    .dataFetcher(new JDBCFetcher("errors"))
                    .build()
            )
            // A field containing all the unique stop sequences (patterns) in this feed.
            .field(newFieldDefinition()
                    .name("patterns")
                    .type(new GraphQLList(patternType))
                    .argument(multiStringArg("pattern_id"))
                    // DataFetchers can either be class instances implementing the interface, or a static function reference
                    .dataFetcher(new JDBCFetcher("patterns"))
                    .build())
            // Then the fields for the sub-tables within the feed (loaded directly from GTFS).
            .field(newFieldDefinition()
                    .name("agency")
                    .type(new GraphQLList(GraphQLGtfsSchema.agencyType))
                    .argument(stringArg("namespace")) // FIXME maybe these nested namespace arguments are not doing anything.
                    .argument(multiStringArg("agency_id"))
                    .argument(intArg(ID_ARG))
                    .argument(intArg(LIMIT_ARG))
                    .argument(intArg(OFFSET_ARG))
                    .dataFetcher(new JDBCFetcher("agency"))
                    .build()
            )
            .field(newFieldDefinition()
                    .name("calendar")
                    .type(new GraphQLList(GraphQLGtfsSchema.calendarType))
                    .argument(stringArg("namespace")) // FIXME maybe these nested namespace arguments are not doing anything.
                    .argument(multiStringArg("service_id"))
                    .argument(intArg(ID_ARG))
                    .argument(intArg(LIMIT_ARG))
                    .argument(intArg(OFFSET_ARG))
                    .dataFetcher(new JDBCFetcher("calendar"))
                    .build()
            )
            .field(newFieldDefinition()
                    .name("fares")
                    .type(new GraphQLList(GraphQLGtfsSchema.fareType))
                    .argument(stringArg("namespace")) // FIXME maybe these nested namespace arguments are not doing anything.
                    .argument(multiStringArg("fare_id"))
                    .argument(intArg(ID_ARG))
                    .argument(intArg(LIMIT_ARG))
                    .argument(intArg(OFFSET_ARG))
                    .dataFetcher(new JDBCFetcher("fare_attributes"))
                    .build()
            )
            .field(newFieldDefinition()
                    .name("routes")
                    .type(new GraphQLList(GraphQLGtfsSchema.routeType))
                    .argument(stringArg("namespace"))
                    .argument(multiStringArg("route_id"))
                    .argument(intArg(ID_ARG))
                    .argument(intArg(LIMIT_ARG))
                    .argument(intArg(OFFSET_ARG))
                    .dataFetcher(new JDBCFetcher("routes"))
                    .build()
            )
            .field(newFieldDefinition()
                    .name("stops")
                    .type(new GraphQLList(GraphQLGtfsSchema.stopType))
                    .argument(stringArg("namespace")) // FIXME maybe these nested namespace arguments are not doing anything.
                    .argument(multiStringArg("stop_id"))
                    .argument(multiStringArg("pattern_id"))
                    .argument(intArg(ID_ARG))
                    .argument(intArg(LIMIT_ARG))
                    .argument(intArg(OFFSET_ARG))
                    .dataFetcher(new JDBCFetcher("stops"))
                    .build()
            )
            .field(newFieldDefinition()
                    .name("trips")
                    .type(new GraphQLList(GraphQLGtfsSchema.tripType))
                    .argument(stringArg("namespace"))
                    .argument(multiStringArg("trip_id"))
                    .argument(multiStringArg("route_id"))
                    .argument(intArg(ID_ARG))
                    .argument(intArg(LIMIT_ARG))
                    .argument(intArg(OFFSET_ARG))
                    .dataFetcher(new JDBCFetcher("trips"))
                    .build()
            )
            .field(newFieldDefinition()
                    .name("stop_times")
                    .type(new GraphQLList(GraphQLGtfsSchema.stopTimeType))
                    .argument(stringArg("namespace"))
                    .argument(intArg(LIMIT_ARG))
                    .argument(intArg(OFFSET_ARG))
                    .dataFetcher(new JDBCFetcher("stop_times"))
                    .build()
            )
            .field(newFieldDefinition()
                    .name("services")
                    .argument(multiStringArg("service_id"))
                    .type(new GraphQLList(GraphQLGtfsSchema.serviceType))
                    .argument(intArg(LIMIT_ARG)) // Todo somehow autogenerate these JDBCFetcher builders to include standard params.
                    .argument(intArg(OFFSET_ARG))
                    .dataFetcher(new JDBCFetcher("services"))
                    .build()
            )
            .build();

    /**
     * This is the top-level query - you must always specify a feed to fetch, and then some other things inside that feed.
     * TODO decide whether to call this feedVersion or feed within gtfs-lib context.
     */
    private static GraphQLObjectType feedQuery = newObject()
            .name("feedQuery")
            .field(newFieldDefinition()
                .name("feed")
                .type(feedType)
                // We scope to a single feed namespace, otherwise GTFS entity IDs are ambiguous.
                .argument(stringArg("namespace"))
                .dataFetcher(new FeedFetcher())
                .build()
            )
            .build();

    /**
     * A top-level query that returns all of the patterns that serve a given stop ID. This demonstrates the use of
     * NestedJDBCFetcher.
     */
//    private static GraphQLObjectType patternsForStopQuery = newObject()
//            .name("patternsForStopQuery")
//            .field(newFieldDefinition()
//                    .name("patternsForStop")
//                    // Field type should be equivalent to the final JDBCFetcher table type.
//                    .type(new GraphQLList(patternType))
//                    // We scope to a single feed namespace, otherwise GTFS entity IDs are ambiguous.
//                    .argument(stringArg("namespace"))
//                    // We allow querying only for a single stop, otherwise result processing can take a long time (lots
//                    // of join queries).
//                    .argument(stringArg("stop_id"))
//                    .dataFetcher(new NestedJDBCFetcher(
//                            new JDBCFetcher("pattern_stops", "stop_id"),
//                            new JDBCFetcher("patterns", "pattern_id")))
//                    .build())
//            .build();


    /**
     * This is the new schema as of July 2017, where all sub-entities are wrapped in a feed.
     * Because all of these fields are static (ugh) this must be declared after the feedQuery it references.
     */
    public static final GraphQLSchema feedBasedSchema = GraphQLSchema
            .newSchema()
            .query(feedQuery)
//            .query(patternsForStopQuery)
            // TODO: Add mutations.
            // .mutation(someMutation)
            .build();


}
