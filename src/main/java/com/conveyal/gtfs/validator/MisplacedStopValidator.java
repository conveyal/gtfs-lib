package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.storage.BooleanAsciiGrid;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import static com.conveyal.gtfs.error.NewGTFSErrorType.STOP_GEOGRAPHIC_OUTLIER;
import static com.conveyal.gtfs.error.NewGTFSErrorType.STOP_LOW_POPULATION_DENSITY;
import static com.conveyal.gtfs.util.Util.getCoordString;

/**
 * This checks whether stops are anywhere outside populated areas (including "null island" or the Sahara) or far away
 * from most other stops in the feed.
 */
public class MisplacedStopValidator extends FeedValidator {

    public MisplacedStopValidator(Feed feed, SQLErrorStorage errorStorage) {
        super(feed, errorStorage);
    }

    @Override
    public void validate() {
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

        BooleanAsciiGrid populationGrid = BooleanAsciiGrid.forEarthPopulation();
        for (Stop stop : feed.stops) {
            boolean stopInPopulatedArea = populationGrid.getValueForCoords(stop.stop_lon, stop.stop_lat);
            if (!stopInPopulatedArea) {
                registerError(STOP_LOW_POPULATION_DENSITY, getCoordString(stop), stop);
            }
            if (stop.stop_lat < minlat || stop.stop_lat > maxlat || stop.stop_lon < minLon || stop.stop_lon > maxLon) {
                registerError(STOP_GEOGRAPHIC_OUTLIER, getCoordString(stop), stop);
            }
        }
    }
}
