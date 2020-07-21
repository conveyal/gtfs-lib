package com.conveyal.gtfs.validator.service;

import com.conveyal.gtfs.util.Util;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.LineString;

/**
 * GeoUtils ported from old GTFS validator, without the MathTransforms.
 */
public class GeoUtils {
  public static double RADIANS = 2 * Math.PI;

  /*
   * Taken from OneBusAway's UTMLibrary class
   */
  @Deprecated
  public static int getUTMZoneForLongitude(double lon) {

    if (lon < -180 || lon > 180)
      throw new IllegalArgumentException(
          "Coordinates not within UTM zone limits");

    int lonZone = (int) ((lon + 180) / 6);

    if (lonZone == 60)
      lonZone--;
    return lonZone + 1;
  }


    /** Get the length of a linestring in meters */
    public static double getDistance(LineString tripGeometry) {
      double distance = 0;
      for (int i = 0; i < tripGeometry.getNumPoints() - 1; i++) {
        distance += Util.fastDistance(
            tripGeometry.getCoordinateN(i).y,
            tripGeometry.getCoordinateN(i).x,
            tripGeometry.getCoordinateN(i + 1).y,
            tripGeometry.getCoordinateN(i + 1).x
        );
      }

      return distance;
    }
}
