package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.error.RouteNameError;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.validator.model.InvalidValue;
import com.conveyal.gtfs.validator.model.Priority;
import com.conveyal.gtfs.validator.model.ValidationResult;

/**
 * Created by landon on 5/2/16.
 */
public class NamesValidator extends GTFSValidator {
    @Override
    public boolean validate(GTFSFeed feed, boolean repair) {
        boolean isValid = true;
        ValidationResult result = new ValidationResult();

        ///////// ROUTES
        for (Route route : feed.routes.values()) {
            String shortName = "";
            String longName = "";
            String desc = "";

            if (route.route_short_name != null)
                shortName = route.route_short_name.trim().toLowerCase();

            if (route.route_long_name != null)
                longName = route.route_long_name.trim().toLowerCase();

            if (route.route_desc != null)
                desc = route.route_desc.toLowerCase();


            //RouteShortAndLongNamesAreBlank
            if (longName.isEmpty() && shortName.isEmpty()) {
                feed.errors.add(new RouteNameError("route", 0, "route_short_name,route_long_name", route.route_id, "RouteShortAndLongNamesAreBlank", route, Priority.HIGH));
                isValid = false;
            }
            //ValidateRouteShortNameIsTooLong
            if (shortName.length() > 6) {
                feed.errors.add(new RouteNameError("route", 0, "route_short_name", route.route_id, "ValidateRouteShortNameIsTooLong", route, Priority.MEDIUM));
                isValid = false;
            }
            //ValidateRouteLongNameContainShortName
            if (!longName.isEmpty() && !shortName.isEmpty() && longName.contains(shortName)) {
                feed.errors.add(new RouteNameError("route", 0, "route_short_name,route_long_name", route.route_id, "ValidateRouteLongNameContainShortName", route, Priority.MEDIUM));
                isValid = false;
            }
            //ValidateRouteDescriptionSameAsRouteName
            if (!desc.isEmpty() && (desc.equals(shortName) || desc.equals(longName))) {
                feed.errors.add(new RouteNameError("route", 0, "route_short_name,route_long_name,route_desc", route.route_id, "ValidateRouteDescriptionSameAsRouteName", route, Priority.MEDIUM));
                isValid = false;
            }
            //ValidateRouteTypeInvalidValid
            if (route.route_type < 0 || route.route_type > 7){
                feed.errors.add(new RouteNameError("route", 0, "route_type", route.route_id, "ValidateRouteTypeInvalidValid", route, Priority.HIGH));
                isValid = false;
            }
        }
        return isValid;
    }
}
