package com.conveyal.gtfs.error;

import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.validator.model.Priority;

import java.io.Serializable;

/**
 * Created by landon on 5/6/16.
 */
public class RouteNameError extends GTFSError implements Serializable {
    public static final long serialVersionUID = 1L;

    public final String type;
    public final Priority priority;
    public final Object problemData;

    public RouteNameError(Route route, String field, String type, Object problemData, Priority priority) {
        super("routes", route.sourceFileLine, field, route.route_id);
        this.type = type;
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
