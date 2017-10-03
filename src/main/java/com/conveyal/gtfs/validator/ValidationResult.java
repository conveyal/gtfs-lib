package com.conveyal.gtfs.validator;

import javafx.geometry.Bounds;

import java.time.LocalDate;
import java.util.Collection;

/**
 * An instance of this class is returned by the validator.
 * It groups together several kinds of information about what happened during the validation process.
 * Detailed lists of errors can be found in database tables created by the validator, but this class provides
 * immediate summary information.
 */
public class ValidationResult {

    public Exception fatalException = null;

    public int errorCount;
    public LocalDate declaredStartDate;
    public LocalDate declaredEndDate;
    public LocalDate firstCalendarDate;
    public LocalDate lastCalendarDate;
    public int[] dailyTravelTime;
    public int[] dailyStopTimes;
    public int[] dailyTrips;
    public GeographicBounds fullBounds = new GeographicBounds();
    public GeographicBounds boundsWithoutOutliers = new GeographicBounds();

    public static class GeographicBounds {
        public double minLon;
        public double minLat;
        public double maxLon;
        public double maxLat;
    }

}
