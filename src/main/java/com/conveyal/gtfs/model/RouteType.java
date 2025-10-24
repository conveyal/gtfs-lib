package com.conveyal.gtfs.model;

public enum RouteType {

    TRAM(0),
    SUBWAY(1),
    RAIL(2),
    BUS(3),
    FERRY(4),
    CABLE_CAR(5),
    GONDOLA(6),
    FUNICULAR(7),
    TROLLEY_BUS(11),
    MONORAIL(12);

    private final int code;

    private RouteType(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static RouteType fromCode(int code) {
        switch (code) {
            case 0:
                return TRAM;
            case 1:
                return SUBWAY;
            case 2:
                return RAIL;
            case 3:
                return BUS;
            case 4:
                return FERRY;
            case 5:
                return CABLE_CAR;
            case 6:
                return GONDOLA;
            case 7:
                return FUNICULAR;
            case 11:
                return TROLLEY_BUS;
            case 12:
                return MONORAIL;
            default:
                throw new IllegalArgumentException("Unknown route type code " + code);
        }
    }
}
