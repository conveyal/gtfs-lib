package com.conveyal.gtfs.model;

import java.util.Optional;

import static com.conveyal.gtfs.model.RouteType.*;

/**
 * Extended route types provide more specificity than the standard 0-7 route types.
 * See https://developers.google.com/transit/gtfs/reference/extended-route-types
 */
public enum ExtendedRouteType {
    // Railway Service (100-199)
    RAILWAY_SERVICE(100, "Railway Service", RAIL),
    HIGH_SPEED_RAIL(101, "High Speed Rail Service", RAIL),
    LONG_DISTANCE_TRAIN(102, "Long Distance Trains", RAIL),
    INTER_REGIONAL_RAIL(103, "Inter Regional Rail Service", RAIL),
    CAR_TRANSPORT_RAIL(104, "Car Transport Rail Service", RAIL),
    SLEEPER_RAIL(105, "Sleeper Rail Service", RAIL),
    REGIONAL_RAIL(106, "Regional Rail Service", RAIL),
    TOURIST_RAIL(107, "Tourist Railway Service", RAIL),
    RAIL_SHUTTLE(108, "Rail Shuttle (Within Complex)", RAIL),
    SUBURBAN_RAIL(109, "Suburban Railway", RAIL),
    REPLACEMENT_RAIL(110, "Replacement Rail Service", RAIL),
    SPECIAL_RAIL(111, "Special Rail Service", RAIL),
    LORRY_TRANSPORT_RAIL(112, "Lorry Transport Rail Service", RAIL),
    ALL_RAIL(113, "All Rail Services", RAIL),
    CROSS_COUNTRY_RAIL(114, "Cross-Country Rail Service", RAIL),
    VEHICLE_TRANSPORT_RAIL(115, "Vehicle Transport Rail Service", RAIL),
    RACK_AND_PINION_RAILWAY(116, "Rack and Pinion Railway", RAIL),
    ADDITIONAL_RAIL(117, "Additional Rail Service", RAIL),

    // Coach Service (200-299)
    COACH_SERVICE(200, "Coach Service", BUS),
    INTERNATIONAL_COACH(201, "International Coach Service", BUS),
    NATIONAL_COACH(202, "National Coach Service", BUS),
    SHUTTLE_COACH(203, "Shuttle Coach Service", BUS),
    REGIONAL_COACH(204, "Regional Coach Service", BUS),
    SPECIAL_COACH(205, "Special Coach Service", BUS),
    SIGHTSEEING_COACH(206, "Sightseeing Coach Service", BUS),
    TOURIST_COACH(207, "Tourist Coach Service", BUS),
    COMMUTER_COACH(208, "Commuter Coach Service", BUS),
    ALL_COACH(209, "All Coach Services", BUS),

    // Suburban Railway Service (300-399)
    SUBURBAN_RAILWAY_SERVICE(300, "Suburban Railway Service", RAIL),

    // Urban Railway Service (400-499)
    URBAN_RAILWAY_SERVICE(400, "Urban Railway Service", RAIL),
    METRO_SERVICE(401, "Metro Service", RouteType.SUBWAY),
    UNDERGROUND_SERVICE(402, "Underground Service", RouteType.SUBWAY),
    URBAN_RAILWAY(403, "Urban Railway Service", RAIL),
    ALL_URBAN_RAILWAY(404, "All Urban Railway Services", RAIL),
    MONORAIL(405, "Monorail", RouteType.MONORAIL),

    // Bus Service (700-799)
    BUS_SERVICE(700, "Bus Service", BUS),
    REGIONAL_BUS(701, "Regional Bus Service", BUS),
    EXPRESS_BUS(702, "Express Bus Service", BUS),
    STOPPING_BUS(703, "Stopping Bus Service", BUS),
    LOCAL_BUS(704, "Local Bus Service", BUS),
    NIGHT_BUS(705, "Night Bus Service", BUS),
    POST_BUS(706, "Post Bus Service", BUS),
    SPECIAL_NEEDS_BUS(707, "Special Needs Bus", BUS),
    MOBILITY_BUS(708, "Mobility Bus Service", BUS),
    MOBILITY_BUS_FOR_REGISTERED_DISABLED(709, "Mobility Bus for Registered Disabled", BUS),
    SIGHTSEEING_BUS(710, "Sightseeing Bus", BUS),
    SHUTTLE_BUS(711, "Shuttle Bus", BUS),
    SCHOOL_BUS(712, "School Bus", BUS),
    SCHOOL_AND_PUBLIC_SERVICE_BUS(713, "School and Public Service Bus", BUS),
    RAIL_REPLACEMENT_BUS(714, "Rail Replacement Bus Service", BUS),
    DEMAND_AND_RESPONSE_BUS(715, "Demand and Response Bus Service", BUS),
    ALL_BUS_SERVICES(716, "All Bus Services", BUS),

    // Trolleybus Service (800-899)
    TROLLEYBUS_SERVICE(800, "Trolleybus Service", BUS),

    // Tram Service (900-999)
    TRAM_SERVICE(900, "Tram Service", TRAM),
    CITY_TRAM(901, "City Tram Service", TRAM),
    LOCAL_TRAM(902, "Local Tram Service", TRAM),
    REGIONAL_TRAM(903, "Regional Tram Service", TRAM),
    SIGHTSEEING_TRAM(904, "Sightseeing Tram Service", TRAM),
    SHUTTLE_TRAM(905, "Shuttle Tram Service", TRAM),
    ALL_TRAM_SERVICES(906, "All Tram Services", TRAM),

    // Water Transport Service (1000-1099)
    WATER_TRANSPORT(1000, "Water Transport Service", FERRY),
    INTERNATIONAL_CAR_FERRY(1001, "International Car Ferry Service", FERRY),
    NATIONAL_CAR_FERRY(1002, "National Car Ferry Service", FERRY),
    REGIONAL_CAR_FERRY(1003, "Regional Car Ferry Service", FERRY),
    LOCAL_CAR_FERRY(1004, "Local Car Ferry Service", FERRY),
    INTERNATIONAL_PASSENGER_FERRY(1005, "International Passenger Ferry Service", FERRY),
    NATIONAL_PASSENGER_FERRY(1006, "National Passenger Ferry Service", FERRY),
    REGIONAL_PASSENGER_FERRY(1007, "Regional Passenger Ferry Service", FERRY),
    LOCAL_PASSENGER_FERRY(1008, "Local Passenger Ferry Service", FERRY),
    POST_BOAT(1009, "Post Boat Service", FERRY),
    TRAIN_FERRY(1010, "Train Ferry Service", FERRY),
    ROAD_LINK_FERRY(1011, "Road-Link Ferry Service", FERRY),
    AIRPORT_LINK_FERRY(1012, "Airport-Link Ferry Service", FERRY),
    CAR_HIGH_SPEED_FERRY(1013, "Car High-Speed Ferry Service", FERRY),
    PASSENGER_HIGH_SPEED_FERRY(1014, "Passenger High-Speed Ferry Service", FERRY),
    SIGHTSEEING_BOAT(1015, "Sightseeing Boat Service", FERRY),
    SCHOOL_BOAT(1016, "School Boat", FERRY),
    CABLE_DRAWN_BOAT(1017, "Cable-Drawn Boat Service", FERRY),
    RIVER_BUS(1018, "River Bus Service", FERRY),
    SCHEDULED_FERRY(1019, "Scheduled Ferry Service", FERRY),
    SHUTTLE_FERRY(1020, "Shuttle Ferry Service", FERRY),
    ALL_WATER_TRANSPORT(1021, "All Water Transport Services", FERRY),

    // Air Service (1100-1199)
    AIR_SERVICE(1100, "Air Service", null),
    INTERNATIONAL_AIR(1101, "International Air Service", null),
    DOMESTIC_AIR(1102, "Domestic Air Service", null),
    INTERCONTINENTAL_AIR(1103, "Intercontinental Air Service", null),
    DOMESTIC_SCHEDULED_AIR(1104, "Domestic Scheduled Air Service", null),
    SHUTTLE_AIR(1105, "Shuttle Air Service", null),
    INTERCONTINENTAL_CHARTER_AIR(1106, "Intercontinental Charter Air Service", null),
    INTERNATIONAL_CHARTER_AIR(1107, "International Charter Air Service", null),
    ROUND_TRIP_CHARTER_AIR(1108, "Round-Trip Charter Air Service", null),
    SIGHTSEEING_AIR(1109, "Sightseeing Air Service", null),
    HELICOPTER_AIR(1110, "Helicopter Air Service", null),
    DOMESTIC_CHARTER_AIR(1111, "Domestic Charter Air Service", null),
    SCHENGEN_AREA_AIR(1112, "Schengen-Area Air Service", null),
    AIRSHIP_SERVICE(1113, "Airship Service", null),
    ALL_AIR_SERVICES(1114, "All Air Services", null),

    // Ferry Service (1200-1299)
    FERRY_SERVICE(1200, "Ferry Service", FERRY),

    // Aerial Lift Service (1300-1399)
    TELECABIN_SERVICE(1300, "Telecabin Service", GONDOLA),
    CABLE_CAR_SERVICE(1301, "Cable Car Service", GONDOLA),
    ELEVATOR_SERVICE(1302, "Elevator Service", GONDOLA),
    CHAIR_LIFT_SERVICE(1303, "Chair Lift Service", GONDOLA),
    DRAG_LIFT_SERVICE(1304, "Drag Lift Service", GONDOLA),
    SMALL_TELECABIN_SERVICE(1305, "Small Telecabin Service", GONDOLA),
    ALL_TELECABIN_SERVICES(1306, "All Telecabin Services", GONDOLA),
    ALL_AERIAL_LIFT_SERVICES(1307, "All Aerial Lift Services", GONDOLA),

    // Funicular Service (1400-1499)
    FUNICULAR_SERVICE(1400, "Funicular Service", FUNICULAR),
    ALL_FUNICULAR_SERVICE(1401, "All Funicular Services", FUNICULAR),

    // Taxi Service (1500-1599)
    TAXI_SERVICE(1500, "Taxi Service", null),
    COMMUNAL_TAXI(1501, "Communal Taxi Service", null),
    WATER_TAXI(1502, "Water Taxi Service", FERRY),
    RAIL_TAXI(1503, "Rail Taxi Service", null),
    BIKE_TAXI(1504, "Bike Taxi Service", null),
    LICENSED_TAXI(1505, "Licensed Taxi Service", null),
    PRIVATE_HIRE_VEHICLE(1506, "Private Hire Service Vehicle", null),
    ALL_TAXI_SERVICES(1507, "All Taxi Services", null),

    // Miscellaneous Service (1700-1799)
    MISC_SERVICE(1700, "Miscellaneous Service", null),
    CABLE_CAR_MISC(1701, "Cable Car", GONDOLA),
    HORSE_DRAWN_CARRIAGE(1702, "Horse-drawn Carriage", null);

    private final int code;
    private final String description;
    private final RouteType standardRouteType;

    ExtendedRouteType(int code, String description, RouteType standardRouteType) {
        this.code = code;
        this.description = description;
        this.standardRouteType = standardRouteType;
    }

    public int getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }

    /**
     * Returns the standard route type (0-7) that this extended type maps to.
     */
    public Optional<RouteType> getStandardRouteType() {
        return Optional.ofNullable(standardRouteType);
    }

    /**
     * Returns the ExtendedRouteType enum for the given code, or null if not found.
     */
    public static ExtendedRouteType fromCode(int code) {
        for (ExtendedRouteType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        return null;
    }
}
