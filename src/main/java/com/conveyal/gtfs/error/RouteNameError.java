package com.conveyal.gtfs.error;

import com.conveyal.gtfs.validator.model.Priority;

/**
 * Created by landon on 5/6/16.
 */
public class RouteNameError extends GTFSError {

    public String affectedEntityId;
    public String type;
    public Priority priority;
    public Object problemData;

    public RouteNameError(String file, long line, String field, String affectedEntityId, String type, Object problemData, Priority priority) {
        super("route", line, field);
        this.affectedEntityId = affectedEntityId;
        this.type = type;
        this.affectedEntityId = affectedEntityId;
        this.priority = priority;
        this.problemData = problemData;
    }

    @Override public String getMessage() {
//        return type;
        switch (type) {
            case "ValidateRouteShortNameIsTooLong":
                return type; // "route_short_name is " + shortName.length() + " chars ('" + shortName + "')"
            case "ValidateRouteLongNameContainShortName":
                return type;
            case "ValidateRouteTypeInvalidValid":
                return type; // "route_type is " + route.route_type
            default:
                return type; // "'" + problemData.longName + "' contains '" + shortName + "'";
        }
    }
}
