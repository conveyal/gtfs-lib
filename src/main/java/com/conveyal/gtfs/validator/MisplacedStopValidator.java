package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.error.GTFSError;
import com.conveyal.gtfs.error.GeneralError;
import com.conveyal.gtfs.error.MisplacedStopError;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.storage.BooleanAsciiGrid;
import com.conveyal.gtfs.validator.service.GeoUtils;
import com.conveyal.gtfs.validator.service.ProjectedCoordinate;
import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.index.strtree.STRtree;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.util.ArrayList;
import java.util.List;

/**
 * This checks whether stops are in "null island" or far away from most other stops.
 * It also attempts to dynamically identify CRS-specific zero-points such as the "null oasis" in the Sahara Desert
 * found in a lot of French data.
 *
 * Should also check if coordinates appear to be reversed, or if they are outside the inhabited zone of the world.
 * We could also just use an image of population density, and check whether each stop falls in a populated area.
 * http://sedac.ciesin.columbia.edu/data/set/gpw-v3-population-density/data-download
 *
 * Should we be doing k-means clustering or something?
 */
public class MisplacedStopValidator extends Validator {

    @Override
    public boolean validate(Feed feed, boolean repair) {

        BooleanAsciiGrid populationGrid = BooleanAsciiGrid.forEarthPopulation();
        for (Stop stop : feed.stops) {
            boolean stopInPopulatedArea = populationGrid.getValueForCoords(stop.stop_lon, stop.stop_lat);
            if (!stopInPopulatedArea) registerError("Stop was in area with < 5 people per square kilometer: " + stop.stop_id);
        }

        // Look for outliers
        DescriptiveStatistics latStats = new DescriptiveStatistics();
        DescriptiveStatistics lonStats = new DescriptiveStatistics();
        for (Stop stop : feed.stops) {
            latStats.addValue(stop.stop_lat);
            lonStats.addValue(stop.stop_lon);
        }

        double latLoP = latStats.getPercentile(10);
        double latHiP = latStats.getPercentile(90);
        double latRange = latHiP - latLoP;
        double minlat = latLoP - latRange;
        double maxlat = latHiP + latRange;

        double lonLoP = lonStats.getPercentile(10);
        double lonHiP = lonStats.getPercentile(90);
        double lonRange = lonHiP - lonLoP;
        double minLon = lonLoP - lonRange;
        double maxLon = lonHiP + lonRange;

        for (Stop stop : feed.stops) {
            if (stop.stop_lat < minlat || stop.stop_lat > maxlat || stop.stop_lon < minLon || stop.stop_lon > maxLon) {
                registerError("Stop is a geographic outlier: " + stop.stop_id);
            }
        }

        return foundErrors();

    }
}
