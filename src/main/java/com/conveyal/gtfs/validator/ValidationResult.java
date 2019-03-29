package com.conveyal.gtfs.validator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.awt.geom.Rectangle2D;
import java.io.Serializable;
import java.time.LocalDate;

/**
 * An instance of this class is returned by the validator.
 * It groups together several kinds of information about what happened during the validation process.
 * Detailed lists of errors can be found in database tables created by the validator, but this class provides
 * immediate summary information.
 *
 * Ignore unknown properties on deserialization to avoid conflicts with past versions. FIXME
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ValidationResult implements Serializable {

    private static final long serialVersionUID = 1L;
    public String fatalException = null;

    public int errorCount;
    public LocalDate declaredStartDate;
    public LocalDate declaredEndDate;

    /**
     * the actual first and last date of service which is calculated in {@link ServiceValidator#complete}
     * These variables are actually not directly tied to data in the calendar_dates.txt file.  Instead, they represent
     * the first and last date respectively of any entry in the calendar.txt and calendar_dates.txt files.
     */
    public LocalDate firstCalendarDate;
    public LocalDate lastCalendarDate;

    public int[] dailyBusSeconds;
    public int[] dailyTramSeconds;
    public int[] dailyMetroSeconds;
    public int[] dailyRailSeconds;
    public int[] dailyTotalSeconds;
    public int[] dailyTripCounts;
    public GeographicBounds fullBounds = new GeographicBounds();
    public GeographicBounds boundsWithoutOutliers = new GeographicBounds();
    public long validationTime;

    public static class GeographicBounds implements Serializable {
        private static final long serialVersionUID = 1L;
        public double minLon;
        public double minLat;
        public double maxLon;
        public double maxLat;

        public Rectangle2D.Double toRectangle2D () {
            return new Rectangle2D.Double(minLon, minLat,
                    maxLon - minLon, maxLat - minLat);
        }

        // FIXME: use JTS instead?
        public void expandToInclude(double stop_lat, double stop_lon) {
//            if (stop_lat < minLat) {
//                minLat = stop_lat;
//            } else if (stop_lat > )

        }
    }

}
