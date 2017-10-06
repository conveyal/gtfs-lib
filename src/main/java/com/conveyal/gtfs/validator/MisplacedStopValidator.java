package com.conveyal.gtfs.validator;

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
        double minLat = latLoP - latRange;
        double maxLat = latHiP + latRange;

        double lonLoP = lonStats.getPercentile(10);
        double lonHiP = lonStats.getPercentile(90);
        double lonRange = lonHiP - lonLoP;
        double minLon = lonLoP - lonRange;
        double maxLon = lonHiP + lonRange;

        // store full bounds in validation result
        ValidationResult.GeographicBounds fullBounds = new ValidationResult.GeographicBounds();
        fullBounds.minLat = latStats.getMin();
        fullBounds.maxLat = latStats.getMax();
        fullBounds.minLon = lonStats.getMin();
        fullBounds.maxLon = lonStats.getMax();

        feed.validationResult.fullBounds = fullBounds;

        // store bounds without outliers in validation result
        ValidationResult.GeographicBounds boundsWithoutOutliers = new ValidationResult.GeographicBounds();
        boundsWithoutOutliers.minLat = minLat;
        boundsWithoutOutliers.maxLat = maxLat;
        boundsWithoutOutliers.minLon = minLon;
        boundsWithoutOutliers.maxLon = maxLon;

        feed.validationResult.boundsWithoutOutliers = boundsWithoutOutliers;


        // determine if a stop is in a low population grid cell or is an outlier
        BooleanAsciiGrid populationGrid = BooleanAsciiGrid.forEarthPopulation();
        for (Stop stop : feed.stops) {
            boolean stopInPopulatedArea = populationGrid.getValueForCoords(stop.stop_lon, stop.stop_lat);
            if (!stopInPopulatedArea) {
                registerError(stop, STOP_LOW_POPULATION_DENSITY, getCoordString(stop));
            }
            if (stop.stop_lat < minLat || stop.stop_lat > maxLat || stop.stop_lon < minLon || stop.stop_lon > maxLon) {
                registerError(stop, STOP_GEOGRAPHIC_OUTLIER, getCoordString(stop));
            }
        }
    }
}
