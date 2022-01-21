package com.conveyal.gtfs.graphql;

import com.conveyal.gtfs.graphql.fetchers.ErrorCountFetcher;
import com.conveyal.gtfs.graphql.fetchers.FeedFetcher;
import com.conveyal.gtfs.graphql.fetchers.JDBCFetcher;
import com.conveyal.gtfs.graphql.fetchers.MapFetcher;
import com.conveyal.gtfs.graphql.fetchers.NestedJDBCFetcher;
import com.conveyal.gtfs.graphql.fetchers.PolylineFetcher;
import com.conveyal.gtfs.graphql.fetchers.RowCountFetcher;
import com.conveyal.gtfs.graphql.fetchers.SQLColumnFetcher;
import com.conveyal.gtfs.graphql.fetchers.SourceObjectFetcher;
import com.conveyal.gtfs.loader.Table;
import graphql.schema.Coercing;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLTypeReference;

import java.sql.Array;
import java.sql.SQLException;

import static com.conveyal.gtfs.graphql.GraphQLUtil.floatArg;
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
    // Additionally, the tree will be explored once top-down following explicit object references, and only
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
            .field(MapFetcher.field("agency_phone"))
            .field(MapFetcher.field("agency_email"))
            .field(MapFetcher.field("agency_lang"))
            .field(MapFetcher.field("agency_fare_url"))
            .field(MapFetcher.field("agency_timezone"))
            .build();

    // Represents rows from booking_rules.txt
    public static final GraphQLObjectType bookingRulesType = newObject().name("booking_rules")
            .description("A GTFS booking rule object")
            .field(MapFetcher.field("id", GraphQLInt))
            .field(MapFetcher.field("booking_rule_id"))
            .field(MapFetcher.field("booking_type", GraphQLInt))
            .field(MapFetcher.field("prior_notice_duration_min", GraphQLInt))
            .field(MapFetcher.field("prior_notice_duration_max", GraphQLInt))
            .field(MapFetcher.field("prior_notice_last_day"))
            .field(MapFetcher.field("prior_notice_last_time"))
            .field(MapFetcher.field("prior_notice_start_day"))
            .field(MapFetcher.field("prior_notice_start_time"))
            .field(MapFetcher.field("prior_notice_service_id"))
            .field(MapFetcher.field("message"))
            .field(MapFetcher.field("pickup_message"))
            .field(MapFetcher.field("drop_off_message"))
            .field(MapFetcher.field("phone_number"))
            .field(MapFetcher.field("info_url"))
            .field(MapFetcher.field("booking_url"))
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
            // FIXME: Description is an editor-specific field
            .field(MapFetcher.field("description"))
            .build();

    private static final GraphQLScalarType stringList = new GraphQLScalarType("StringList", "List of Strings", new StringCoercing());

    // Represents GTFS Editor service exceptions.
    public static final GraphQLObjectType scheduleExceptionType = newObject().name("scheduleException")
            .description("A GTFS Editor schedule exception type")
            .field(MapFetcher.field("id", GraphQLInt))
            .field(MapFetcher.field("name"))
            .field(MapFetcher.field("exemplar", GraphQLInt))
            .field(MapFetcher.field("dates", stringList))
            .field(MapFetcher.field("custom_schedule", stringList))
            .field(MapFetcher.field("added_service", stringList))
            .field(MapFetcher.field("removed_service", stringList))
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
            .description("A GTFS fare attributes object")
            .field(MapFetcher.field("id", GraphQLInt))
            .field(MapFetcher.field("agency_id"))
            .field(MapFetcher.field("fare_id"))
            .field(MapFetcher.field("price", GraphQLFloat))
            .field(MapFetcher.field("currency_type"))
            .field(MapFetcher.field("payment_method", GraphQLInt))
            .field(MapFetcher.field("transfers", GraphQLInt))
            .field(MapFetcher.field("transfer_duration", GraphQLInt))
            .field(newFieldDefinition()
                    .name("fare_rules")
                    .type(new GraphQLList(fareRuleType))
                    // FIXME Update JDBCFetcher to have noLimit boolean for fetchers on "naturally" nested types
                    // (i.e., nested types that typically would only be nested under another entity and only make sense
                    // with the entire set -- fares -> fare rules, trips -> stop times, patterns -> pattern stops/shapes)
                    .argument(intArg(LIMIT_ARG))
                    .dataFetcher(new JDBCFetcher("fare_rules", "fare_id"))
                    .build()
            )
            .build();

    // Represents feed_info.txt
    public static final GraphQLObjectType feedInfoType = newObject().name("feed_info")
            .description("A GTFS feed_info object")
            .field(MapFetcher.field("id", GraphQLInt))
            .field(MapFetcher.field("feed_id"))
            .field(MapFetcher.field("feed_contact_email"))
            .field(MapFetcher.field("feed_contact_url"))
            .field(MapFetcher.field("feed_publisher_name"))
            .field(MapFetcher.field("feed_publisher_url"))
            .field(MapFetcher.field("feed_lang"))
            .field(MapFetcher.field("default_lang"))
            .field(MapFetcher.field("feed_start_date"))
            .field(MapFetcher.field("feed_end_date"))
            .field(MapFetcher.field("feed_version"))
            // Editor-specific fields
            .field(MapFetcher.field("default_route_color"))
            .field(MapFetcher.field("default_route_type"))
            .build();

    // Represents location_groups.txt
    public static final GraphQLObjectType locationGroupType = newObject().name("location_groups")
            .description("A GTFS location_group object")
            .field(MapFetcher.field("id", GraphQLInt))
            .field(MapFetcher.field("location_group_id"))
            .field(MapFetcher.field("location_id"))
            .field(MapFetcher.field("location_group_name"))
            .build();

    // Represents the shapes held within locations.geojson
    public static final GraphQLObjectType locationShapeType = newObject().name("location_shapes")
            .description("A GTFS location_shape object")
            .field(MapFetcher.field("id", GraphQLInt))
            .field(MapFetcher.field("location_id"))
            .field(MapFetcher.field("geometry_id"))
            .field(MapFetcher.field("geometry_type"))
            .field(MapFetcher.field("geometry_pt_lat", GraphQLFloat))
            .field(MapFetcher.field("geometry_pt_lon", GraphQLFloat))

//            .field(MapFetcher.field("shape_id"))
//            .field(MapFetcher.field("shape_polygon_id"))
//            .field(MapFetcher.field("shape_ring_id"))
//            .field(MapFetcher.field("shape_line_string_id"))
//            .field(MapFetcher.field("shape_pt_lat"))
//            .field(MapFetcher.field("shape_pt_lon"))
//            .field(MapFetcher.field("shape_pt_sequence"))
//            .field(MapFetcher.field("location_meta_data_id"))
            .build();

    // Represents more 'meta' data from locations.geoJSON
    public static final GraphQLObjectType locationsType = newObject().name("locations")
            .description("A GTFS locations object")
            .field(MapFetcher.field("id", GraphQLInt))
            .field(MapFetcher.field("location_id"))
            .field(MapFetcher.field("stop_name"))
            .field(MapFetcher.field("stop_desc"))
            .field(MapFetcher.field("zone_id"))
            .field(MapFetcher.field("stop_url"))
            .field(MapFetcher.field("geometry_type"))
            .field(newFieldDefinition()
                    .name("location_shapes")
                    .type(new GraphQLList(locationShapeType))
                    // FIXME Update JDBCFetcher to have noLimit boolean for fetchers on "naturally" nested types
                    // (i.e., nested types that typically would only be nested under another entity and only make sense
                    // with the entire set -- fares -> fare rules, trips -> stop times, patterns -> pattern stops/shapes)
                    .argument(intArg(LIMIT_ARG))
                    .dataFetcher(new JDBCFetcher("location_shapes", "location_id"))
                    .build()
            )
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

    // Represents a set of rows from shapes.txt joined by shape_id
    public static final GraphQLObjectType shapeEncodedPolylineType = newObject().name("shapeEncodedPolyline")
        .field(string("shape_id"))
        .field(string("polyline"))
        .build();


    // Represents rows from frequencies.txt
    public static final GraphQLObjectType frequencyType =  newObject().name("frequency")
            .field(MapFetcher.field("trip_id"))
            .field(MapFetcher.field("start_time", GraphQLInt))
            .field(MapFetcher.field("end_time", GraphQLInt))
            .field(MapFetcher.field("headway_secs", GraphQLInt))
            .field(MapFetcher.field("exact_times", GraphQLInt))
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
            .field(MapFetcher.field("wheelchair_accessible", GraphQLInt))
            .field(MapFetcher.field("bikes_allowed", GraphQLInt))
            .field(MapFetcher.field("shape_id"))
            .field(MapFetcher.field("pattern_id"))
            .field(newFieldDefinition()
                    .name("stop_times")
                    // forward reference to the as yet undefined stopTimeType (must be defined
                    // after tripType)
                    .type(new GraphQLList(new GraphQLTypeReference("stopTime")))
                    // FIXME Update JDBCFetcher to have noLimit boolean for fetchers on "naturally"
                    //  nested types (i.e., nested types that typically would only be nested under
                    //  another entity and only make sense with the entire set -- fares -> fare
                    //  rules, trips -> stop times, patterns -> pattern stops/shapes)
                    .argument(intArg(LIMIT_ARG))
                    .dataFetcher(new JDBCFetcher(
                            "stop_times",
                            "trip_id",
                            "stop_sequence",
                            false))
                    .build()
            )
            .field(newFieldDefinition()
                    .name("frequencies")
                    // forward reference to the as yet undefined stopTimeType (must be defined after tripType)
                    .type(new GraphQLList(frequencyType))
                    // FIXME Update JDBCFetcher to have noLimit boolean for fetchers on "naturally" nested types
                    // (i.e., nested types that typically would only be nested under another entity and only make sense
                    // with the entire set -- fares -> fare rules, trips -> stop times, patterns -> pattern stops/shapes)
                    .argument(intArg(LIMIT_ARG))
                    .dataFetcher(new JDBCFetcher("frequencies", "trip_id"))
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
            .field(MapFetcher.field("arrival_time", GraphQLInt))
            .field(MapFetcher.field("departure_time", GraphQLInt))
            .field(MapFetcher.field("stop_id"))
            .field(MapFetcher.field("stop_sequence", GraphQLInt))
            .field(MapFetcher.field("stop_headsign"))
            .field(MapFetcher.field("pickup_type", GraphQLInt))
            .field(MapFetcher.field("drop_off_type", GraphQLInt))
            .field(MapFetcher.field("continuous_drop_off", GraphQLInt))
            .field(MapFetcher.field("continuous_pickup", GraphQLInt))
            .field(MapFetcher.field("shape_dist_traveled", GraphQLFloat))
            .field(MapFetcher.field("timepoint", GraphQLInt))
            // Additional GTFS Flex booking rule fields.
            .field(MapFetcher.field("pickup_booking_rule_id"))
            .field(MapFetcher.field("drop_off_booking_rule_id"))
            // Additional GTFS Flex location groups and locations fields
            .field(MapFetcher.field("start_pickup_dropoff_window", GraphQLInt))
            .field(MapFetcher.field("end_pickup_dropoff_window", GraphQLInt))
            .field(MapFetcher.field("mean_duration_factor", GraphQLFloat))
            .field(MapFetcher.field("mean_duration_offset", GraphQLFloat))
            .field(MapFetcher.field("safe_duration_factor", GraphQLFloat))
            .field(MapFetcher.field("safe_duration_offset", GraphQLFloat))
            .build();

    // Represents rows from attributions.txt
    public static final GraphQLObjectType attributionsType = newObject().name("attributions")
        .field(MapFetcher.field("attribution_id"))
        .field(MapFetcher.field("agency_id"))
        .field(MapFetcher.field("route_id"))
        .field(MapFetcher.field("trip_id"))
        .field(MapFetcher.field("organization_name"))
        .field(MapFetcher.field("is_producer", GraphQLInt))
        .field(MapFetcher.field("is_operator", GraphQLInt))
        .field(MapFetcher.field("is_authority", GraphQLInt))
        .field(MapFetcher.field("attribution_url"))
        .field(MapFetcher.field("attribution_email"))
        .field(MapFetcher.field("attribution_phone"))
        .build();

    // Represents rows from translations.txt
    public static final GraphQLObjectType translationsType = newObject().name("translations")
        .field(MapFetcher.field("table_name"))
        .field(MapFetcher.field("field_name"))
        .field(MapFetcher.field("language"))
        .field(MapFetcher.field("translation"))
        .field(MapFetcher.field("record_id"))
        .field(MapFetcher.field("record_sub_id"))
        .field(MapFetcher.field("field_value"))
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
            .field(MapFetcher.field("continuous_drop_off", GraphQLInt))
            .field(MapFetcher.field("continuous_pickup", GraphQLInt))
            .field(MapFetcher.field("route_type", GraphQLInt))
            .field(MapFetcher.field("route_color"))
            .field(MapFetcher.field("route_text_color"))
            // FIXME ˇˇ Editor fields that should perhaps be moved elsewhere.
            .field(MapFetcher.field("wheelchair_accessible"))
            .field(MapFetcher.field("publicly_visible", GraphQLInt))
            .field(MapFetcher.field("status", GraphQLInt))
            .field(MapFetcher.field("route_sort_order"))
            // FIXME ^^
            .field(RowCountFetcher.field("trip_count", "trips", "route_id"))
            .field(RowCountFetcher.field("pattern_count", "patterns", "route_id"))
            .field(newFieldDefinition()
                    .name("stops")
                    .description("GTFS stop entities that the route serves")
                    // Field type should be equivalent to the final JDBCFetcher table type.
                    .type(new GraphQLList(new GraphQLTypeReference("stop")))
                    // We scope to a single feed namespace, otherwise GTFS entity IDs are ambiguous.
                    .argument(stringArg("namespace"))
                    .argument(stringArg(SEARCH_ARG))
                    .argument(intArg(LIMIT_ARG))
                    // We allow querying only for a single stop, otherwise result processing can take a long time (lots
                    // of join queries).
                    .argument(stringArg("route_id"))
                    .dataFetcher(new NestedJDBCFetcher(
                            // If it is not possible to join across all fetches (e.g. no matching id) an empty array
                            // will be returned.
                            new JDBCFetcher("patterns", "route_id", null, false),
                            new JDBCFetcher("pattern_stops", "pattern_id", null, false),
                            new JDBCFetcher("pattern_locations", "pattern_id", null, false),
                            new JDBCFetcher("stops", "stop_id")))
                    .build())
            .field(newFieldDefinition()
                    .type(new GraphQLList(tripType))
                    .name("trips")
                    .argument(multiStringArg("trip_id"))
                    // FIXME Update JDBCFetcher to have noLimit boolean for fetchers on "naturally" nested types
                    // (i.e., nested types that typically would only be nested under another entity and only make sense
                    // with the entire set -- fares -> fare rules, trips -> stop times, patterns -> pattern stops/shapes)
                    .argument(intArg(LIMIT_ARG))
                    .argument(stringArg(DATE_ARG))
                    .argument(intArg(FROM_ARG))
                    .argument(intArg(TO_ARG))
                    .dataFetcher(new JDBCFetcher("trips", "route_id"))
                    .build()
            )
            .field(newFieldDefinition()
                    .type(new GraphQLList(new GraphQLTypeReference("pattern")))
                    .name("patterns")
                    .argument(intArg(LIMIT_ARG))
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
            .field(MapFetcher.field("platform_code"))
            .field(MapFetcher.field("location_type", GraphQLInt))
            .field(MapFetcher.field("wheelchair_boarding", GraphQLInt))
            // Returns all stops that reference parent stop's stop_id
            .field(newFieldDefinition()
                    .name("child_stops")
                    .type(new GraphQLList(new GraphQLTypeReference("stop")))
                    .dataFetcher(new JDBCFetcher(
                        "stops",
                        "stop_id",
                        null,
                        false,
                        "parent_station"
                    ))
                    .build())
            .field(RowCountFetcher.field("stop_time_count", "stop_times", "stop_id"))
            .field(newFieldDefinition()
                    .name("patterns")
                    // Field type should be equivalent to the final JDBCFetcher table type.
                    .type(new GraphQLList(new GraphQLTypeReference("pattern")))
                    .argument(stringArg("namespace"))
                    .dataFetcher(new NestedJDBCFetcher(
                            // If it is not possible to join across all fetches (e.g. no matching id) an empty array
                            // will be returned.
                            new JDBCFetcher("pattern_stops", "stop_id", null, false),
                            new JDBCFetcher("patterns", "pattern_id"),
                            new JDBCFetcher("pattern_locations", "location_id")))
                    .build())
            .field(newFieldDefinition()
                    .name("routes")
                    // Field type should be equivalent to the final JDBCFetcher table type.
                    .type(new GraphQLList(routeType))
                    .argument(stringArg("namespace"))
                    .argument(stringArg(SEARCH_ARG))
                    .argument(intArg(LIMIT_ARG))
                    .dataFetcher(new NestedJDBCFetcher(
                            // If it is not possible to join across all fetches (e.g. no matching id) an empty array
                            // will be returned.
                            new JDBCFetcher("pattern_stops", "stop_id", null, false),
                            new JDBCFetcher("pattern_locations", "location_id", null, false),
                            new JDBCFetcher("patterns", "pattern_id", null, false),
                            new JDBCFetcher("routes", "route_id")))
                    .build())
            .field(newFieldDefinition()
                    .name("stop_times")
                    // forward reference to the as yet undefined stopTimeType (must be defined after tripType)
                    .type(new GraphQLList(new GraphQLTypeReference("stopTime")))
                    // FIXME Update JDBCFetcher to have noLimit boolean for fetchers on "naturally" nested types
                    // (i.e., nested types that typically would only be nested under another entity and only make sense
                    // with the entire set -- fares -> fare rules, trips -> stop times, patterns -> pattern stops/shapes)
                    .argument(intArg(LIMIT_ARG))
                    .dataFetcher(new JDBCFetcher("stop_times", "stop_id", "stop_sequence", false)))
            // Flex locations location_group

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
            .field(MapFetcher.field("drop_off_booking_rule_id"))
            .field(MapFetcher.field("pickup_booking_rule_id"))
            .field(MapFetcher.field("default_travel_time", GraphQLInt))
            .field(MapFetcher.field("default_dwell_time", GraphQLInt))
            .field(MapFetcher.field("shape_dist_traveled", GraphQLFloat))
            .field(MapFetcher.field("drop_off_type", GraphQLInt))
            .field(MapFetcher.field("pickup_type", GraphQLInt))
            .field(MapFetcher.field("continuous_drop_off", GraphQLInt))
            .field(MapFetcher.field("continuous_pickup", GraphQLInt))
            .field(MapFetcher.field("stop_sequence", GraphQLInt))
            .field(MapFetcher.field("timepoint", GraphQLInt))
            .field(MapFetcher.field("pickup_booking_rule_id"))
            .field(MapFetcher.field("drop_off_booking_rule_id"))
        // FIXME: This will only returns a list with one stop entity (unless there is a referential integrity issue)
            // Should this be modified to be an object, rather than a list?
            .field(newFieldDefinition()
                    .type(new GraphQLList(stopType))
                    .name("stop")
                    .dataFetcher(new JDBCFetcher("stops", "stop_id"))
                    .build()
            )
            .build();

    /**
     * Represents each stop in a list of locations within a pattern.
     * We could return just a list of LocationIDs within the pattern (a JSON array of strings) but
     * that structure would prevent us from joining tables and returning additional stop details
     * like lat and lon, or pickup and dropoff types if we add those to the pattern signature.
     */
    public static final GraphQLObjectType patternLocationType = newObject().name("patternLocation")
            .field(MapFetcher.field("id", GraphQLInt))
            .field(MapFetcher.field("pattern_id"))
            .field(MapFetcher.field("location_id"))
            .field(MapFetcher.field("drop_off_booking_rule_id"))
            .field(MapFetcher.field("pickup_booking_rule_id"))
            .field(MapFetcher.field("shape_dist_traveled", GraphQLFloat))
            .field(MapFetcher.field("drop_off_type", GraphQLInt))
            .field(MapFetcher.field("pickup_type", GraphQLInt))
            .field(MapFetcher.field("continuous_drop_off", GraphQLInt))
            .field(MapFetcher.field("continuous_pickup", GraphQLInt))
            .field(MapFetcher.field("stop_sequence", GraphQLInt))
            .field(MapFetcher.field("timepoint", GraphQLInt))
            .field(MapFetcher.field("pickup_booking_rule_id"))
            .field(MapFetcher.field("drop_off_booking_rule_id"))

            // Additional GTFS Flex location groups and locations fields
            .field(MapFetcher.field("flex_default_travel_time", GraphQLInt))
            .field(MapFetcher.field("flex_default_zone_time", GraphQLInt))
            .field(MapFetcher.field("mean_duration_factor", GraphQLFloat))
            .field(MapFetcher.field("mean_duration_offset", GraphQLFloat))
            .field(MapFetcher.field("safe_duration_factor", GraphQLFloat))
            .field(MapFetcher.field("safe_duration_offset", GraphQLFloat))
            // FIXME: This will only returns a list with one stop entity (unless there is a referential integrity issue)
            // Should this be modified to be an object, rather than a list?
            .field(newFieldDefinition()
                    .type(new GraphQLList(locationsType))
                    .name("location")
                    .dataFetcher(new JDBCFetcher("locations", "location_id"))
                    .build()
            )
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

    public static GraphQLObjectType tripGroupCountType = newObject().name("tripGroupCount")
            .description("")
            .field(RowCountFetcher.groupedField("trips", "service_id"))
            .field(RowCountFetcher.groupedField("trips", "route_id"))
            .field(RowCountFetcher.groupedField("trips", "pattern_id"))
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
            .field(string("priority"))
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
            // FIXME: Fields directly below are editor-specific. Move somewhere else?
            .field(MapFetcher.field("direction_id", GraphQLInt))
            .field(MapFetcher.field("use_frequency", GraphQLInt))
            .field(MapFetcher.field("name"))
            .field(newFieldDefinition()
                    .name("shape")
                    .type(new GraphQLList(shapePointType))
                    // FIXME Update JDBCFetcher to have noLimit boolean for fetchers on "naturally" nested types
                    // (i.e., nested types that typically would only be nested under another entity and only make sense
                    // with the entire set -- fares -> fare rules, trips -> stop times, patterns -> pattern stops/shapes)
                    .argument(intArg(LIMIT_ARG))
                    .dataFetcher(new JDBCFetcher("shapes",
                            "shape_id",
                            "shape_pt_sequence",
                            false))
                    .build())
            .field(RowCountFetcher.field("trip_count", "trips", "pattern_id"))
            .field(newFieldDefinition()
                .name("pattern_stops")
                .type(new GraphQLList(patternStopType))
                // FIXME Update JDBCFetcher to have noLimit boolean for fetchers on "naturally" nested types
                // (i.e., nested types that typically would only be nested under another entity and only make sense
                // with the entire set -- fares -> fare rules, trips -> stop times, patterns -> pattern stops/shapes)
                .argument(intArg(LIMIT_ARG))
                .dataFetcher(new JDBCFetcher("pattern_stops",
                        "pattern_id",
                        "stop_sequence",
                        false))
                .build())
            .field(newFieldDefinition()
                    .name("pattern_locations")
                    .type(new GraphQLList(patternLocationType))
                    // FIXME Update JDBCFetcher to have noLimit boolean for fetchers on "naturally" nested types
                    // (i.e., nested types that typically would only be nested under another entity and only make sense
                    // with the entire set -- fares -> fare rules, trips -> stop times, patterns -> pattern stops/shapes)
                    .argument(intArg(LIMIT_ARG))
                    .dataFetcher(new JDBCFetcher("pattern_locations",
                            "pattern_id",
                            "stop_sequence",
                            false))
                    .build())
            .field(newFieldDefinition()
                .name("stops")
                .description("GTFS stop entities that the pattern serves")
                // Field type should be equivalent to the final JDBCFetcher table type.
                .type(new GraphQLList(stopType))
                // We scope to a single feed namespace, otherwise GTFS entity IDs are ambiguous.
                .argument(stringArg("namespace"))
                .argument(intArg(LIMIT_ARG))
                // We allow querying only for a single stop, otherwise result processing can take a long time (lots
                // of join queries).
                .argument(stringArg("pattern_id"))
                .dataFetcher(new NestedJDBCFetcher(
                        // If it is not possible to join across all fetches (e.g. no matching id) an empty array
                        // will be returned.
                        new JDBCFetcher("pattern_stops", "pattern_id", null, false),
                        new JDBCFetcher("pattern_locations", "location_id", null, false),
                        new JDBCFetcher("stops", "stop_id")))
                .build())
            .field(newFieldDefinition()
                .name("trips")
                .type(new GraphQLList(tripType))
                // FIXME Update JDBCFetcher to have noLimit boolean for fetchers on "naturally" nested types
                // (i.e., nested types that typically would only be nested under another entity and only make sense
                // with the entire set -- fares -> fare rules, trips -> stop times, patterns -> pattern stops/shapes)
                .argument(intArg(LIMIT_ARG))
                .argument(stringArg(DATE_ARG))
                .argument(intArg(FROM_ARG))
                .argument(intArg(TO_ARG))
                .argument(multiStringArg("service_id"))
                .dataFetcher(new JDBCFetcher("trips", "pattern_id"))
                .build())
            // FIXME This is a singleton array because the JdbcFetcher currently only works with one-to-many joins.
            .field(newFieldDefinition()
                .name("route")
                // Field type should be equivalent to the final JDBCFetcher table type.
                .type(new GraphQLList(routeType))
                .argument(stringArg("namespace"))
                .dataFetcher(new JDBCFetcher("routes", "route_id"))
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
                    // FIXME Update JDBCFetcher to have noLimit boolean for fetchers on "naturally" nested types
                    // (i.e., nested types that typically would only be nested under another entity and only make sense
                    // with the entire set -- fares -> fare rules, trips -> stop times, patterns -> pattern stops/shapes)
                    .type(new GraphQLList(GraphQLString))
                    .dataFetcher(new SQLColumnFetcher<String>("service_dates", "service_id", "service_date"))
                    .build())
            .field(newFieldDefinition()
                    .name("trips")
                    // FIXME Update JDBCFetcher to have noLimit boolean for fetchers on "naturally" nested types
                    // (i.e., nested types that typically would only be nested under another entity and only make sense
                    // with the entire set -- fares -> fare rules, trips -> stop times, patterns -> pattern stops/shapes)
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
            .field(MapFetcher.field("md5"))
            .field(MapFetcher.field("sha1"))
            .field(MapFetcher.field("filename"))
            .field(MapFetcher.field("loaded_date"))
            .field(MapFetcher.field("snapshot_of"))
            // A field containing row counts for every table.
            .field(newFieldDefinition()
                    .name("row_counts")
                    .type(rowCountsType)
                    .dataFetcher(new SourceObjectFetcher())
                    .build())
            .field(newFieldDefinition()
                    .name("trip_counts")
                    .type(tripGroupCountType)
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
            // A field containing the feed info table.
            .field(newFieldDefinition()
                    .name("feed_info")
                    .type(new GraphQLList(feedInfoType))
                    // FIXME: These arguments really don't make sense for feed info, but in order to create generic
                    // fetches on the client-side they have been included here.
                    .argument(intArg(ID_ARG))
                    .argument(intArg(LIMIT_ARG))
                    .argument(intArg(OFFSET_ARG))
                    // DataFetchers can either be class instances implementing the interface, or a static function reference
                    .dataFetcher(new JDBCFetcher("feed_info"))
                    .build())
            // A field containing all the unique stop sequences (patterns) in this feed.
            .field(newFieldDefinition()
                    .name("patterns")
                    .type(new GraphQLList(patternType))
                    .argument(intArg(ID_ARG))
                    .argument(intArg(LIMIT_ARG))
                    .argument(intArg(OFFSET_ARG))
                    .argument(floatArg(MIN_LAT))
                    .argument(floatArg(MIN_LON))
                    .argument(floatArg(MAX_LAT))
                    .argument(floatArg(MAX_LON))
                    .argument(multiStringArg("pattern_id"))
                    // DataFetchers can either be class instances implementing the interface, or a static function reference
                    .dataFetcher(new JDBCFetcher("patterns"))
                    .build())
            .field(newFieldDefinition()
                .name("shapes_as_polylines")
                .type(new GraphQLList(shapeEncodedPolylineType))
                // DataFetchers can either be class instances implementing the interface, or a static function reference
                .dataFetcher(new PolylineFetcher())
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
                    .name("booking_rules")
                    .type(new GraphQLList(GraphQLGtfsSchema.bookingRulesType))
                    .argument(stringArg("namespace")) // FIXME maybe these nested namespace arguments are not doing anything.
                    .argument(multiStringArg("booking_rule_id"))
                    .argument(intArg(ID_ARG))
                    .argument(intArg(LIMIT_ARG))
                    .argument(intArg(OFFSET_ARG))
                    .dataFetcher(new JDBCFetcher(Table.BOOKING_RULES.name))
                    .build()
            )
            .field(newFieldDefinition()
                    .name("locations")
                    .type(new GraphQLList(GraphQLGtfsSchema.locationsType))
                    .argument(stringArg("namespace")) // FIXME maybe these nested namespace arguments are not doing anything.
                    .argument(multiStringArg("location_id"))
                    .argument(intArg(ID_ARG))
                    .argument(intArg(LIMIT_ARG))
                    .argument(intArg(OFFSET_ARG))
                    .dataFetcher(new JDBCFetcher(Table.LOCATIONS.name))
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
                .name("location_groups")
                .type(new GraphQLList(GraphQLGtfsSchema.locationGroupType))
                .argument(stringArg("namespace")) // FIXME maybe these nested namespace arguments are not doing anything.
                .argument(multiStringArg("location_group_id"))
                .argument(intArg(ID_ARG))
                .argument(intArg(LIMIT_ARG))
                .argument(intArg(OFFSET_ARG))
                .dataFetcher(new JDBCFetcher(Table.LOCATION_GROUPS.name))
                .build()
            )
            .field(newFieldDefinition()
                .name("location_shapes")
                .type(new GraphQLList(GraphQLGtfsSchema.locationShapeType))
                .argument(stringArg("namespace")) // FIXME maybe these nested namespace arguments are not doing anything.
                .argument(multiStringArg("shape_id"))
                .argument(intArg(ID_ARG))
                .argument(intArg(LIMIT_ARG))
                .argument(intArg(OFFSET_ARG))
                .dataFetcher(new JDBCFetcher(Table.LOCATION_SHAPES.name))
                .build()
            )
            .field(newFieldDefinition()
                    .name("routes")
                    .type(new GraphQLList(GraphQLGtfsSchema.routeType))
                    .argument(stringArg("namespace"))
                    .argument(multiStringArg("route_id"))
                    .argument(stringArg(SEARCH_ARG))
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
                    .argument(floatArg(MIN_LAT))
                    .argument(floatArg(MIN_LON))
                    .argument(floatArg(MAX_LAT))
                    .argument(floatArg(MAX_LON))
                    .argument(stringArg(SEARCH_ARG))
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
                    .argument(stringArg(DATE_ARG))
                    .argument(intArg(FROM_ARG))
                    .argument(intArg(TO_ARG))
                    .argument(intArg(OFFSET_ARG))
                    .dataFetcher(new JDBCFetcher("trips"))
                    .build()
            )
            .field(newFieldDefinition()
                    .name("schedule_exceptions")
                    .type(new GraphQLList(GraphQLGtfsSchema.scheduleExceptionType))
                    .argument(stringArg("namespace"))
                    .argument(intArg(ID_ARG))
                    .argument(intArg(LIMIT_ARG))
                    .argument(intArg(OFFSET_ARG))
                    .dataFetcher(new JDBCFetcher("schedule_exceptions"))
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
            .field(newFieldDefinition()
                .name("attributions")
                .type(new GraphQLList(GraphQLGtfsSchema.attributionsType))
                .argument(stringArg("namespace")) // FIXME maybe these nested namespace arguments are not doing anything.
                .argument(intArg(ID_ARG))
                .argument(intArg(LIMIT_ARG))
                .argument(intArg(OFFSET_ARG))
                .dataFetcher(new JDBCFetcher("attributions"))
                .build()
            )
            .field(newFieldDefinition()
                .name("translations")
                .type(new GraphQLList(GraphQLGtfsSchema.translationsType))
                .argument(stringArg("namespace")) // FIXME maybe these nested namespace arguments are not doing anything.
                .argument(intArg(ID_ARG))
                .argument(intArg(LIMIT_ARG))
                .argument(intArg(OFFSET_ARG))
                .dataFetcher(new JDBCFetcher("translations"))
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
            .build();


    private static class StringCoercing implements Coercing {
        @Override
        public Object serialize(Object input) {
            String[] strings = new String[]{};
            try {
                strings = (String[])((Array) input).getArray();
//                if (strings == null) strings = new String[]{};
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return strings;
        }

        @Override
        public Object parseValue(Object input) {
            return null;
        }

        @Override
        public Object parseLiteral(Object input) {
            return null;
        }
    }
}
