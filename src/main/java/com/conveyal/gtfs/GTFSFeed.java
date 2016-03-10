package com.conveyal.gtfs;

import com.conveyal.gtfs.error.GTFSError;
import com.conveyal.gtfs.model.*;
import com.conveyal.gtfs.validator.GTFSValidator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.vividsolutions.jts.geom.*;
import org.geotools.referencing.GeodeticCalculator;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Fun;
import org.mapdb.Fun.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import static com.conveyal.gtfs.util.Util.human;

/**
 * All entities must be from a single feed namespace.
 * Composed of several GTFSTables.
 */
public class GTFSFeed implements Cloneable, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(GTFSFeed.class);

    DB db = DBMaker.newTempFileDB()
            .transactionDisable()
            .mmapFileEnable()
            .asyncWriteEnable()
            .deleteFilesAfterClose()
            .compressionEnable()
            // .cacheSize(1024 * 1024) this bloats memory consumption, as do in-memory maps below.
            .make(); // TODO db.close();

    public String feedId = null;

    // TODO make all of these Maps MapDBs so the entire GTFSFeed is persistent and uses constant memory

    /* Some of these should be multimaps since they don't have an obvious unique key. */
    public final Map<String, Agency>        agency         = Maps.newHashMap();
    public final Map<String, FeedInfo>      feedInfo       = Maps.newHashMap();
    public final Map<String, Frequency>     frequencies    = Maps.newHashMap();
    public final Map<String, Route>         routes         = Maps.newHashMap();
    public final Map<String, Stop>          stops          = Maps.newHashMap();
    public final Map<String, Transfer>      transfers      = Maps.newHashMap();
    public final Map<String, Trip>          trips          = Maps.newHashMap();

    /* Map from 2-tuples of (shape_id, shape_pt_sequence) to shape points */
    public final ConcurrentNavigableMap<Tuple2, Shape> shapePoints = db.getTreeMap("shapes");

    /* This represents a bunch of views of the previous, one for each shape */
    public final Map<String, Map<Integer, Shape>> shapes = Maps.newHashMap();

    /* Map from 2-tuples of (trip_id, stop_sequence) to stoptimes. */
    public final ConcurrentNavigableMap<Tuple2, StopTime> stop_times = db.getTreeMap("stop_times");

    /* A fare is a fare_attribute and all fare_rules that reference that fare_attribute. */
    public final Map<String, Fare> fares = Maps.newHashMap();

    /* A service is a calendar entry and all calendar_dates that modify that calendar entry. */
    public final Map<String, Service> services = Maps.newHashMap();

    /* A place to accumulate errors while the feed is loaded. Tolerate as many errors as possible and keep on loading. */
    public List<GTFSError> errors = Lists.newArrayList();

    /* Create geometry factory to produce LineString geometries. */
    GeometryFactory gf = new GeometryFactory();

    /* Map routes to associated trip patterns. */
    // TODO: Hash Multimapping in guava (might need dependency).
    public final Map<String, Pattern> patterns = Maps.newHashMap();

    public final Map<String, String> tripPatternMap = Maps.newHashMap();

    /**
     * The order in which we load the tables is important for two reasons.
     * 1. We must load feed_info first so we know the feed ID before loading any other entities. This could be relaxed
     * by having entities point to the feed object rather than its ID String.
     * 2. Referenced entities must be loaded before any entities that reference them. This is because we check
     * referential integrity while the files are being loaded. This is done on the fly during loading because it allows
     * us to associate a line number with errors in objects that don't have any other clear identifier.
     *
     * Interestingly, all references are resolvable when tables are loaded in alphabetical order.
     */
    private void loadFromFile(ZipFile zip) throws Exception {
        new FeedInfo.Loader(this).loadTable(zip);
        // maybe we should just point to the feed object itself instead of its ID, and null out its stoptimes map after loading
        if (feedId == null) {
            feedId = new File(zip.getName()).getName().replaceAll("\\.zip$", "");
            LOG.info("Feed ID is undefined, pester maintainers to include a feed ID. Using file name {}.", feedId); // TODO log an error, ideally feeds should include a feedID
        }
        else {
            LOG.info("Feed ID is '{}'.", feedId);
        }

        new Agency.Loader(this).loadTable(zip);
        new Calendar.Loader(this).loadTable(zip);
        new CalendarDate.Loader(this).loadTable(zip);
        new FareAttribute.Loader(this).loadTable(zip);
        new FareRule.Loader(this).loadTable(zip);
        new Route.Loader(this).loadTable(zip);
        new Shape.Loader(this).loadTable(zip);
        new Stop.Loader(this).loadTable(zip);
        new Transfer.Loader(this).loadTable(zip);
        new Trip.Loader(this).loadTable(zip);
        new Frequency.Loader(this).loadTable(zip);
        new StopTime.Loader(this).loadTable(zip); // comment out this line for quick testing using NL feed
        LOG.info("{} errors", errors.size());
        for (GTFSError error : errors) {
            LOG.info("{}", error);
        }
    }

    public void toFile (String file) {
        try {
            File out = new File(file);
            OutputStream os = new FileOutputStream(out);
            ZipOutputStream zip = new ZipOutputStream(os);

            // write everything
            // TODO: fare attributes, fare rules, shapes
            new Agency.Writer(this).writeTable(zip);
            new Calendar.Writer(this).writeTable(zip);
            new CalendarDate.Writer(this).writeTable(zip);
            new Frequency.Writer(this).writeTable(zip);
            new Route.Writer(this).writeTable(zip);
            new Stop.Writer(this).writeTable(zip);
            new Shape.Writer(this).writeTable(zip);
            new Transfer.Writer(this).writeTable(zip);
            new Trip.Writer(this).writeTable(zip);
            new StopTime.Writer(this).writeTable(zip);

            zip.close();

            LOG.info("GTFS file written");
        } catch (Exception e) {
            LOG.error("Error saving GTFS: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void validate (GTFSValidator... validators) {
        for (GTFSValidator validator : validators) {
            validator.validate(this, false);
        }
    }

    public static GTFSFeed fromFile(String file) {
        GTFSFeed feed = new GTFSFeed();
        ZipFile zip;
        try {
            zip = new ZipFile(file);
            feed.loadFromFile(zip);
            zip.close();
            return feed;
        } catch (Exception e) {
            LOG.error("Error loading GTFS: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * For the given trip ID, fetch all the stop times in order of increasing stop_sequence.
     * This is an efficient iteration over a tree map.
     */
    public Iterable<StopTime> getOrderedStopTimesForTrip (String trip_id) {
        Map<Fun.Tuple2, StopTime> tripStopTimes =
                stop_times.subMap(
                        Fun.t2(trip_id, null),
                        Fun.t2(trip_id, Fun.HI)
                );
        return tripStopTimes.values();
    }

    /**
     * For the given trip ID, fetch all the stop times in order, and interpolate stop-to-stop travel times.
     */
    public Iterable<StopTime> getInterpolatedStopTimesForTrip (String trip_id) throws FirstAndLastStopsDoNotHaveTimes {
        // clone stop times so as not to modify base GTFS structures
        StopTime[] stopTimes = StreamSupport.stream(getOrderedStopTimesForTrip(trip_id).spliterator(), false)
                .map(st -> st.clone())
                .toArray(i -> new StopTime[i]);

        // avoid having to make sure that the array has length below.
        if (stopTimes.length == 0) return Collections.emptyList();

        // first pass: set all partially filled stop times
        for (StopTime st : stopTimes) {
            if (st.arrival_time != Entity.INT_MISSING && st.departure_time == Entity.INT_MISSING) {
                st.departure_time = st.arrival_time;
            }

            if (st.arrival_time == Entity.INT_MISSING && st.departure_time != Entity.INT_MISSING) {
                st.arrival_time = st.departure_time;
            }
        }

        // quick check: ensure that first and last stops have times.
        // technically GTFS requires that both arrival_time and departure_time be filled at both the first and last stop,
        // but we are slightly more lenient and only insist that one of them be filled at both the first and last stop.
        // The meaning of the first stop's arrival time is unclear, and same for the last stop's departure time (except
        // in the case of interlining).

        // it's fine to just check departure time, as the above pass ensures that all stop times have either both
        // arrival and departure times, or neither
        if (stopTimes[0].departure_time == Entity.INT_MISSING || stopTimes[stopTimes.length - 1].departure_time == Entity.INT_MISSING) {
            throw new FirstAndLastStopsDoNotHaveTimes();
        }

        // second pass: fill complete stop times
        int startOfInterpolatedBlock = -1;
        for (int stopTime = 0; stopTime < stopTimes.length; stopTime++) {

            if (stopTimes[stopTime].departure_time == Entity.INT_MISSING && startOfInterpolatedBlock == -1) {
                startOfInterpolatedBlock = stopTime;
            }
            else if (stopTimes[stopTime].departure_time != Entity.INT_MISSING && startOfInterpolatedBlock != -1) {
                // we have found the end of the interpolated section
                int nInterpolatedStops = stopTime - startOfInterpolatedBlock;
                double totalLengthOfInterpolatedSection = 0;
                double[] lengthOfInterpolatedSections = new double[nInterpolatedStops];

                GeodeticCalculator calc = new GeodeticCalculator();

                for (int stopTimeToInterpolate = startOfInterpolatedBlock, i = 0; stopTimeToInterpolate < stopTime; stopTimeToInterpolate++, i++) {
                    Stop start = stops.get(stopTimes[stopTimeToInterpolate - 1].stop_id);
                    Stop end = stops.get(stopTimes[stopTimeToInterpolate].stop_id);
                    calc.setStartingGeographicPoint(start.stop_lon, start.stop_lat);
                    calc.setDestinationGeographicPoint(end.stop_lon, end.stop_lat);
                    double segLen = calc.getOrthodromicDistance();
                    totalLengthOfInterpolatedSection += segLen;
                    lengthOfInterpolatedSections[i] = segLen;
                }

                // add the segment post-last-interpolated-stop
                Stop start = stops.get(stopTimes[stopTime - 1].stop_id);
                Stop end = stops.get(stopTimes[stopTime].stop_id);
                calc.setStartingGeographicPoint(start.stop_lon, start.stop_lat);
                calc.setDestinationGeographicPoint(end.stop_lon, end.stop_lat);
                totalLengthOfInterpolatedSection += calc.getOrthodromicDistance();

                int departureBeforeInterpolation = stopTimes[startOfInterpolatedBlock - 1].departure_time;
                int arrivalAfterInterpolation = stopTimes[stopTime].arrival_time;
                int totalTime = arrivalAfterInterpolation - departureBeforeInterpolation;

                double lengthSoFar = 0;
                for (int stopTimeToInterpolate = startOfInterpolatedBlock, i = 0; stopTimeToInterpolate < stopTime; stopTimeToInterpolate++, i++) {
                    lengthSoFar += lengthOfInterpolatedSections[i];

                    int time = (int) (departureBeforeInterpolation + totalTime * (lengthSoFar / totalLengthOfInterpolatedSection));
                    stopTimes[stopTimeToInterpolate].arrival_time = stopTimes[stopTimeToInterpolate].departure_time = time;
                }

                // we're done with this block
                startOfInterpolatedBlock = -1;
            }
        }

        return Arrays.asList(stopTimes);
    }

    public List<String> getOrderedStopListForTrip (String trip_id) {
        Iterable<StopTime> orderedStopTimes = getOrderedStopTimesForTrip(trip_id);
        List<String> stops = Lists.newArrayList();
        // In-order traversal of StopTimes within this trip. The 2-tuple keys determine ordering.
        for (StopTime stopTime : orderedStopTimes) {
            stops.add(stopTime.stop_id);
        }
        return stops;
    }

    /**
     *  Bin all trips by the sequence of stops they visit.
     * @return A map from a list of stop IDs to a list of Trip IDs that visit those stops in that sequence.
     */
    public Map<List<String>, List<String>> findPatterns() {
        // A map from a list of stop IDs (the pattern) to a list of trip IDs which fit that pattern.
        Map<List<String>, List<String>> tripsForPattern = Maps.newHashMap();
        int n = 0;
        for (String trip_id : trips.keySet()) {
            if (++n % 100000 == 0) {
                LOG.info("trip {}", human(n));
            }

            List<String> stops = getOrderedStopListForTrip(trip_id);

            // Fetch or create the tripId list for this stop pattern, then add the current trip to that list.
            List<String> trips = tripsForPattern.get(stops);
            if (trips == null) {
                trips = Lists.newArrayList();
                tripsForPattern.put(stops, trips);
            }
            trips.add(trip_id);
        }
        LOG.info("Total patterns: {}", tripsForPattern.keySet().size());

        for (Entry<List<String>, List<String>> entry: tripsForPattern.entrySet()){
            Pattern pattern = new Pattern(this, entry);
            patterns.put(pattern.pattern_id, pattern);
            entry.getValue().forEach(tripId -> tripPatternMap.put(tripId, pattern.pattern_id));
        }

        return tripsForPattern;
    }

    /**
     * Returns a trip geometry object (LineString) for a given trip id.
     * If the trip has a shape reference, this will be used for the geometry.
     * Otherwise, the ordered stoptimes will be used.
     *
     * @param   trip_id   trip id of desired trip geometry
     * @return          the LineString representing the trip geometry.
     * @see             LineString
     */
    public LineString getTripGeometry(String trip_id){

        CoordinateList coordinates = new CoordinateList();
        LineString ls;
        Trip trip = trips.get(trip_id);

        // If trip has shape_id, use it to generate geometry.
        if (trip.shape_id != null){
            for (Entry<Integer, Shape> entry : trip.shape_points.entrySet()){
                Double lat = entry.getValue().shape_pt_lat;
                Double lon = entry.getValue().shape_pt_lon;
                coordinates.add(new Coordinate(lon, lat));
            }
            ls = gf.createLineString(coordinates.toCoordinateArray());
        }
        // Use the ordered stoptimes.
        else{
            Iterable<StopTime> stopTimes;
            stopTimes = getOrderedStopTimesForTrip(trip.trip_id);
            if (Iterables.size(stopTimes) > 1) {
                for (StopTime stopTime : stopTimes) {
                    Stop stop = stops.get(stopTime.stop_id);
                    Double lat = stop.stop_lat;
                    Double lon = stop.stop_lon;
                    coordinates.add(new Coordinate(lon, lat));
                }
                ls = gf.createLineString(coordinates.toCoordinateArray());
            }
            // set ls equal to null if there is only one stopTime to avoid an exception when creating linestring
            else{
                ls = null;
            }
        }

        return ls;
    }

    public Service getOrCreateService(String serviceId) {
        Service service = services.get(serviceId);
        if (service == null) {
            service = new Service(serviceId);
            services.put(serviceId, service);
        }
        return service;
    }

    public Fare getOrCreateFare(String fareId) {
        Fare fare = fares.get(fareId);
        if (fare == null) {
            fare = new Fare(fareId);
            fares.put(fareId, fare);
        }
        return fare;
    }

    /**
     * Cloning can be useful when you want to make only a few modifications to an existing feed.
     * Keep in mind that this is a shallow copy, so you'll have to create new maps in the clone for tables you want
     * to modify.
     */
    @Override
    public GTFSFeed clone() {
        try {
            return (GTFSFeed) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    public void close () {
        db.close();
    }

    // TODO augment with unrolled calendar, patterns, etc. before validation

    /** Thrown when we cannot interpolate stop times because the first or last stops do not have times */
    public class FirstAndLastStopsDoNotHaveTimes extends Exception {
        /** do nothing */
    }
}
