package com.conveyal.gtfs.validator;

import java.awt.geom.Rectangle2D;
import java.io.Serializable;
import com.conveyal.gtfs.model.Pattern;
import java.time.LocalDate;
import java.util.List;

/**
 * An instance of this class is returned by the validator.
 * It groups together several kinds of information about what happened during the validation process.
 * Detailed lists of errors can be found in database tables created by the validator, but this class provides
 * immediate summary information.
 */
public class ValidationResult implements Serializable {

    private static final long serialVersionUID = 1L;
    public String fatalException = null;

    public int errorCount;
    public LocalDate declaredStartDate;
    public LocalDate declaredEndDate;
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
