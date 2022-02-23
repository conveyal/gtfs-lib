package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.FareRule;
import com.conveyal.gtfs.model.Location;
import com.conveyal.gtfs.model.LocationGroup;
import com.conveyal.gtfs.model.Stop;

/**
 * Validate additional flex files as defined here: https://github.com/MobilityData/gtfs-flex/blob/master/spec/reference.md
 */
public class FlexValidator extends FeedValidator {
    public FlexValidator(Feed feed, SQLErrorStorage errorStorage) {
        super(feed, errorStorage);
    }

    @Override
    public void validate() {
        validateLocationGroups();
        validateLocations();
    }

    /**
     * A {@link LocationGroup#location_group_id} must not match a {@link Stop#stop_id} or a {@link Location#location_id}
     */
    private void validateLocationGroups() {
        for (LocationGroup locationGroup : feed.locationGroups) {
            for(Location location : feed.locations) {
                if (locationGroup.location_group_id.equals(location.location_id)) {
                    registerError(locationGroup, NewGTFSErrorType.FLEX_FORBIDDEN_LOCATION_GROUP_ID, location.location_id);
                }
            }
            for (Stop stop : feed.stops) {
                if (locationGroup.location_group_id.equals(stop.stop_id)) {
                    registerError(locationGroup, NewGTFSErrorType.FLEX_FORBIDDEN_LOCATION_GROUP_ID, stop.stop_id);
                }
            }
        }
    }

    /**
     * 1) A {@link Location#location_id} must not match a {@link Stop#stop_id}.
     * 2) A {@link Location#zone_id} is conditionally required if {@link com.conveyal.gtfs.model.FareRule}s are defined.
     */
    private void validateLocations() {
        for (Location location : feed.locations) {
            for (Stop stop : feed.stops) {
                if (location.location_id.equals(stop.stop_id)) {
                    registerError(location, NewGTFSErrorType.FLEX_FORBIDDEN_LOCATION_ID, stop.stop_id);
                }
            }
        }
        if (hasFareRules()) {
            for (Location location : feed.locations) {
                boolean hasFareRule = false;
                for (FareRule fareRule : feed.fareRules) {
                    if (
                        location.zone_id.equals(fareRule.contains_id) ||
                        location.zone_id.equals(fareRule.destination_id) ||
                        location.zone_id.equals(fareRule.origin_id)
                    ) {
                        hasFareRule = true;
                        break;
                    }
                }
                if (!hasFareRule) {
                    registerError(location, NewGTFSErrorType.FLEX_MISSING_FARE_RULE, location.zone_id);
                }
            }
        }
    }

    /**
     * A {@link Location#location_id} must not match a {@link Stop#stop_id}.
     * A {@link Location#zone_id} is conditionally required if {@link com.conveyal.gtfs.model.FareRule}s are defined.
     * https://github.com/MobilityData/gtfs-flex/blob/master/spec/reference.md#stop_timestxt-file-extended
     */
    private void validateStopTimes() {

    }

    /**
     * Table reader can only be iterated over. This method checks if any fare rules have been defined.
     */
    private boolean hasFareRules() {
        for (FareRule ignored : feed.fareRules) {
            return true;
        }
        return false;
    }

}
