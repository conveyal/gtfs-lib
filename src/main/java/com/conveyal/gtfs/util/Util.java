package com.conveyal.gtfs.util;

import com.conveyal.gtfs.model.Stop;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.apache.commons.math3.util.FastMath;

/**
 * The methods and classes in this package should eventually be part of a shared Conveyal library.
 */
public abstract class Util {

    public static GeometryFactory geometryFactory = new GeometryFactory();

    public static final double METERS_PER_DEGREE_LATITUDE = 111111.111;

    public static String human (int n) {
        if (n >= 1000000000) return String.format("%.1fG", n/1000000000.0);
        if (n >= 1000000) return String.format("%.1fM", n/1000000.0);
        if (n >= 1000) return String.format("%dk", n/1000);
        else return String.format("%d", n);
    }

    public static double yMetersForLat (double latDegrees) {
        return latDegrees * METERS_PER_DEGREE_LATITUDE;
    }

    public static double xMetersForLon (double latDegrees, double lonDegrees) {
        double xScale = FastMath.cos(FastMath.toRadians(latDegrees));
        return xScale * lonDegrees * METERS_PER_DEGREE_LATITUDE;
    }

    public static Coordinate projectLatLonToMeters (double lat, double lon) {
        return new Coordinate(xMetersForLon(lon, lat), yMetersForLat(lat));
    }

    public static String getCoordString(Stop stop) {
        return String.format("lat=%f; lon=%f", stop.stop_lat, stop.stop_lon);
    }

    /**
     * @return Equirectangular approximation to distance.
     */
    public static double fastDistance (double lat0, double lon0, double lat1, double lon1) {
        double midLat = (lat0 + lat1) / 2;
        double xscale = FastMath.cos(FastMath.toRadians(midLat));
        double dx = xscale * (lon1 - lon0);
        double dy = (lat1 - lat0);
        return FastMath.sqrt(dx * dx + dy * dy) * METERS_PER_DEGREE_LATITUDE;
    }

}
