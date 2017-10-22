package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;

import java.util.List;

/**
 * Makes one object representing each service ID.
 * That object will contain a calendar (for repeating service on specific days of the week)
 * and potentially multiple CalendarDates defining exceptions to the base calendar.
 *
 * TODO build histogram of stop times, check against calendar and declared feed validity dates
 */
public class ServiceValidator extends TripValidator {

    public ServiceValidator(Feed feed, SQLErrorStorage errorStorage) {
        super(feed, errorStorage);
    }

    @Override
    public void validateTrip(Trip trip, Route route, List<StopTime> stopTimes, List<Stop> stops) {

    }


}
