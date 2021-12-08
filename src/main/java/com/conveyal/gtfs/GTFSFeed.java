package com.conveyal.gtfs;

import com.conveyal.gtfs.error.GTFSError;
import com.conveyal.gtfs.loader.FeedLoadResult;
import com.conveyal.gtfs.loader.JdbcGTFSFeedConverter;
import com.conveyal.gtfs.model.*;
import com.conveyal.gtfs.model.Calendar;
import com.conveyal.gtfs.validator.Validator;
import com.conveyal.gtfs.util.Util;
import com.conveyal.gtfs.validator.service.GeoUtils;
import com.google.common.collect.*;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ExecutionError;
import org.locationtech.jts.algorithm.ConvexHull;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.index.strtree.STRtree;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Fun;
import org.mapdb.Fun.Tuple2;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOError;
import java.io.IOException;
import java.io.OutputStream;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

/**
 * All entities must be from a single feed namespace.
 * Composed of several GTFSTables.
 */
public class GTFSFeed implements Cloneable, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(GTFSFeed.class);
    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");

    private DB db;

    public String feedId = null;

    // TODO make all of these Maps MapDBs so the entire GTFSFeed is persistent and uses constant memory

    /* Some of these should be multimaps since they don't have an obvious unique key. */
    public final Map<String, Agency> agency;
    public final Map<String, BookingRule> bookingRules;
    public final Map<String, LocationGroup> locationGroups;
    public final Map<String, LocationMetaData> locationMetaData;
    public final Map<String, LocationShape> locationShapes;
    public final Map<String, FeedInfo> feedInfo;
    // This is how you do a multimap in mapdb: https://github.com/jankotek/MapDB/blob/release-1.0/src/test/java/examples/MultiMap.java
    public final NavigableSet<Tuple2<String, Frequency>> frequencies;
    public final Map<String, Route> routes;
    public final Map<String, Stop> stops;
    public final Map<String, Transfer> transfers;
    public final BTreeMap<String, Trip> trips;
    public final Map<String, Translation> translations;
    public final Map<String, Attribution> attributions;
    public final Map<String, Calendar> calenders;

    public final Set<String> transitIds = new HashSet<>();
    /** CRC32 of the GTFS file this was loaded from */
    public long checksum;

    /* Map from 2-tuples of (shape_id, shape_pt_sequence) to shape points */
    public final ConcurrentNavigableMap<Tuple2<String, Integer>, ShapePoint> shape_points;

    /* Map from 2-tuples of (trip_id, stop_sequence) to stoptimes. */
    public final BTreeMap<Tuple2, StopTime> stop_times;

    /* A fare is a fare_attribute and all fare_rules that reference that fare_attribute. */
    public final Map<String, Fare> fares;

    /* A service is a calendar entry and all calendar_dates that modify that calendar entry. */
    public final BTreeMap<String, Service> services;

    /* A place to accumulate errors while the feed is loaded. Tolerate as many errors as possible and keep on loading. */
    public final NavigableSet<GTFSError> errors;

    /* Stops spatial index which gets built lazily by getSpatialIndex() */
    private transient STRtree spatialIndex;

    /* Convex hull of feed (based on stops) built lazily by getConvexHull() */
    private transient Polygon convexHull;

    /* Merged stop buffers polygon built lazily by getMergedBuffers() */
    private transient Geometry mergedBuffers;

    /* Create geometry factory to produce LineString geometries. */
    GeometryFactory gf = new GeometryFactory();

    /* Map routes to associated trip patterns. */
    // TODO: Hash Multimapping in guava (might need dependency).
    public final Map<String, Pattern> patterns;

    // TODO bind this to map above so that it is kept up to date automatically
    public final Map<String, String> tripPatternMap;
    private boolean loaded = false;

    /* A place to store an event bus that is passed through constructor. */
    public transient EventBus eventBus;

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
    public void loadFromFile(ZipFile zip, String fid) throws Exception {
        if (this.loaded) throw new UnsupportedOperationException("Attempt to load GTFS into existing database");

        // NB we don't have a single CRC for the file, so we combine all the CRCs of the component files. NB we are not
        // simply summing the CRCs because CRCs are (I assume) uniformly randomly distributed throughout the width of a
        // long, so summing them is a convolution which moves towards a Gaussian with mean 0 (i.e. more concentrated
        // probability in the center), degrading the quality of the hash. Instead we XOR. Assuming each bit is independent,
        // this will yield a nice uniformly distributed result, because when combining two bits there is an equal
        // probability of any input, which means an equal probability of any output. At least I think that's all correct.
        // Repeated XOR is not commutative but zip.stream returns files in the order they are in the central directory
        // of the zip file, so that's not a problem.
        checksum = zip.stream().mapToLong(ZipEntry::getCrc).reduce((l1, l2) -> l1 ^ l2).getAsLong();

        db.getAtomicLong("checksum").set(checksum);

        new FeedInfo.Loader(this).loadTable(zip);
        // maybe we should just point to the feed object itself instead of its ID, and null out its stoptimes map after loading
        if (fid != null) {
            feedId = fid;
            LOG.info("Feed ID is undefined, pester maintainers to include a feed ID. Using file name {}.", feedId); // TODO log an error, ideally feeds should include a feedID
        }
        else if (feedId == null || feedId.isEmpty()) {
            feedId = new File(zip.getName()).getName().replaceAll("\\.zip$", "");
            LOG.info("Feed ID is undefined, pester maintainers to include a feed ID. Using file name {}.", feedId); // TODO log an error, ideally feeds should include a feedID
        }
        else {
            LOG.info("Feed ID is '{}'.", feedId);
        }

        db.getAtomicString("feed_id").set(feedId);

        new Agency.Loader(this).loadTable(zip);

        // calendars and calendar dates are joined into services. This means a lot of manipulating service objects as
        // they are loaded; since mapdb keys/values are immutable, load them in memory then copy them to MapDB once
        // we're done loading them
        Map<String, Service> serviceTable = new HashMap<>();
        new Calendar.Loader(this, serviceTable).loadTable(zip);
        new CalendarDate.Loader(this, serviceTable).loadTable(zip);
        this.services.putAll(serviceTable);
        serviceTable = null; // free memory

        // Same deal
        Map<String, Fare> fares = new HashMap<>();
        new FareAttribute.Loader(this, fares).loadTable(zip);
        new FareRule.Loader(this, fares).loadTable(zip);
        this.fares.putAll(fares);
        fares = null; // free memory

        // Flex tables. These must be loaded before stop times. If any of these tables contain data it is assumed that
        // we are working with a flex feed.
        new BookingRule.Loader(this).loadTable(zip);
        new LocationGroup.Loader(this).loadTable(zip);
        new LocationMetaData.Loader(this).loadTable(zip);
        new LocationShape.Loader(this).loadTable(zip);

        new Route.Loader(this).loadTable(zip);
        new ShapePoint.Loader(this).loadTable(zip);
        new Stop.Loader(this).loadTable(zip);
        new Transfer.Loader(this).loadTable(zip);
        new Trip.Loader(this).loadTable(zip);
        new Frequency.Loader(this).loadTable(zip);
        new StopTime.Loader(this).loadTable(zip); // comment out this line for quick testing using NL feed

        LOG.info("{} errors", errors.size());
        for (GTFSError error : errors) {
            LOG.info("{}", error);
        }
        loaded = true;
    }

    public void loadFromFile(ZipFile zip) throws Exception {
        loadFromFile(zip, null);
    }

    public FeedLoadResult toSQL (DataSource dataSource) {
        JdbcGTFSFeedConverter converter = new JdbcGTFSFeedConverter(this, dataSource);
        return converter.loadTables();
    }

    public void toFile (String file) {
        try {
            File out = new File(file);
            OutputStream os = new FileOutputStream(out);
            ZipOutputStream zip = new ZipOutputStream(os);

            // write everything
            // TODO: shapes

            // don't write empty feed_info.txt
            if (!this.feedInfo.isEmpty()) new FeedInfo.Writer(this).writeTable(zip);

            new Agency.Writer(this).writeTable(zip);
            new Calendar.Writer(this).writeTable(zip);
            new CalendarDate.Writer(this).writeTable(zip);
            new FareAttribute.Writer(this).writeTable(zip);
            new FareRule.Writer(this).writeTable(zip);
            new Frequency.Writer(this).writeTable(zip);
            new Route.Writer(this).writeTable(zip);
            new Stop.Writer(this).writeTable(zip);
            new ShapePoint.Writer(this).writeTable(zip);
            new Transfer.Writer(this).writeTable(zip);
            new Trip.Writer(this).writeTable(zip);
            new StopTime.Writer(this).writeTable(zip);

            if (!this.bookingRules.isEmpty()) new BookingRule.Writer(this).writeTable(zip);
            if (!this.locationGroups.isEmpty()) new LocationGroup.Writer(this).writeTable(zip);

            zip.close();

            LOG.info("GTFS file written");
        } catch (Exception e) {
            LOG.error("Error saving GTFS: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void validate (boolean repair, Validator... validators) {
        long startValidation = System.currentTimeMillis();
        for (Validator validator : validators) {
            try {
                long startValidator = System.currentTimeMillis();
//                validator.validate(this, repair);
                long endValidator = System.currentTimeMillis();
                long diff = endValidator - startValidator;
                LOG.info("{} finished in {} milliseconds.", validator.getClass().getSimpleName(), TimeUnit.NANOSECONDS.toMillis(diff));
            } catch (Exception e) {
                LOG.error("Could not run {} validator.", validator.getClass().getSimpleName());
//                LOG.error(e.toString());
                e.printStackTrace();
            }
        }
        long endValidation = System.nanoTime();
        long total = endValidation - startValidation;
        LOG.info("{} validators completed in {} milliseconds.", validators.length, TimeUnit.NANOSECONDS.toMillis(total));
    }

    // validate function call that should explicitly list each validator to run on GTFSFeed
    public void validate () {
/////////////////
    }

    /**
     * Static factory method returning a new instance of GTFSFeed containing the contents of
     * the GTFS file at the supplied filesystem path.
     */
    public static GTFSFeed fromFile(String file) {
        return fromFile(file, null);
    }

    /**
     * Static factory method returning a new instance of GTFSFeed containing the contents of
     * the GTFS file at the supplied filesystem path. Forces the feedId to the
     * supplied value if the parameter is non-null.
     */
    public static GTFSFeed fromFile(String file, String feedId) {
        GTFSFeed feed = new GTFSFeed();
        ZipFile zip;
        try {
            zip = new ZipFile(file);
            if (feedId == null) {
                feed.loadFromFile(zip);
            }
            else {
                feed.loadFromFile(zip, feedId);
            }
            zip.close();
            return feed;
        } catch (Exception e) {
            LOG.error("Error loading GTFS: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public boolean hasFeedInfo () {
        return !this.feedInfo.isEmpty();
    }

    public FeedInfo getFeedInfo () {
        return this.hasFeedInfo() ? this.feedInfo.values().iterator().next() : null;
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
     * TODO rename getStopSpatialIndex to make it clear what the index contains.
     */
    public STRtree getSpatialIndex () {
        if (this.spatialIndex == null) {
            synchronized (this) {
                if (this.spatialIndex == null) {
                    // build spatial index
                    STRtree stopIndex = new STRtree();
                    for(Stop stop : this.stops.values()) {
                        try {
                            if (Double.isNaN(stop.stop_lat) || Double.isNaN(stop.stop_lon)) {
                                continue;
                            }
                            Coordinate stopCoord = new Coordinate(stop.stop_lat, stop.stop_lon);
                            stopIndex.insert(new Envelope(stopCoord), stop);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }
                    try {
                        stopIndex.build();
                        this.spatialIndex = stopIndex;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return this.spatialIndex;
    }

    /** Get the shape for the given shape ID */
    public Shape getShape (String shape_id) {
        Shape shape = new Shape(this, shape_id);
        return shape.shape_dist_traveled.length > 0 ? shape : null;
    }

    /**
     * MapDB-based implementation to find patterns.
     *
     * FIXME: Remove and make pattern finding happen during validation? We want to share the pattern finder between the
     * two implementations (MapDB and RDBMS), apply the same validation process to both kinds of storage, and produce
     * Patterns in the same way in both cases, during validation. This prevents us from iterating over every stopTime
     * twice, since we're already iterating over all of them in validation. However, in this case it might not be costly
     * to simply retrieve the stop times from the stop_times map.
     */
    public void findPatterns () {
        PatternFinder patternFinder = new PatternFinder();
        // Iterate over trips and process each trip and its stop times.
        for (Trip trip : this.trips.values()) {
            Iterable<StopTime> orderedStopTimesForTrip = this.getOrderedStopTimesForTrip(trip.trip_id);
            patternFinder.processTrip(trip, orderedStopTimesForTrip);
        }
        Map<TripPatternKey, Pattern> patternObjects = patternFinder.createPatternObjects(this.stops, null);
        this.patterns.putAll(patternObjects.values().stream()
                .collect(Collectors.toMap(Pattern::getId, pattern -> pattern)));
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

                for (int stopTimeToInterpolate = startOfInterpolatedBlock, i = 0; stopTimeToInterpolate < stopTime; stopTimeToInterpolate++, i++) {
                    Stop start = stops.get(stopTimes[stopTimeToInterpolate - 1].stop_id);
                    Stop end = stops.get(stopTimes[stopTimeToInterpolate].stop_id);
                    double segLen = Util.fastDistance(start.stop_lat, start.stop_lon, end.stop_lat, end.stop_lon);
                    totalLengthOfInterpolatedSection += segLen;
                    lengthOfInterpolatedSections[i] = segLen;
                }

                // add the segment post-last-interpolated-stop
                Stop start = stops.get(stopTimes[stopTime - 1].stop_id);
                Stop end = stops.get(stopTimes[stopTime].stop_id);
                totalLengthOfInterpolatedSection += Util.fastDistance(start.stop_lat, start.stop_lon, end.stop_lat, end.stop_lon);

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

    public Collection<Frequency> getFrequencies (String trip_id) {
        // IntelliJ tells me all these casts are unnecessary, and that's also my feeling, but the code won't compile
        // without them
        return (List<Frequency>) frequencies.subSet(new Fun.Tuple2(trip_id, null), new Fun.Tuple2(trip_id, Fun.HI)).stream()
                .map(t2 -> ((Tuple2<String, Frequency>) t2).b)
                .collect(Collectors.toList());
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

    public LineString getStraightLineForStops(String trip_id) {
        CoordinateList coordinates = new CoordinateList();
        LineString ls = null;
        Trip trip = trips.get(trip_id);

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
        return ls;
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
        LineString ls = null;
        Trip trip = trips.get(trip_id);

        // If trip has shape_id, use it to generate geometry.
        if (trip.shape_id != null) {
            Shape shape = getShape(trip.shape_id);
            if (shape != null) ls = shape.geometry;
        }

        // Use the ordered stoptimes.
        if (ls == null) {
            ls = getStraightLineForStops(trip_id);
        }

        return ls;
    }

    /** Get the length of a trip in meters. */
    public double getTripDistance (String trip_id, boolean straightLine) {
        return straightLine
                ? GeoUtils.getDistance(this.getStraightLineForStops(trip_id))
                : GeoUtils.getDistance(this.getTripGeometry(trip_id));
    }

    /** Get trip speed (using trip shape if available) in meters per second. */
    public double getTripSpeed (String trip_id) {
        return getTripSpeed(trip_id, false);
    }

    /** Get trip speed in meters per second. */
    public double getTripSpeed (String trip_id, boolean straightLine) {

        StopTime firstStopTime = this.stop_times.ceilingEntry(Fun.t2(trip_id, null)).getValue();
        StopTime lastStopTime = this.stop_times.floorEntry(Fun.t2(trip_id, Fun.HI)).getValue();

        // ensure that stopTime returned matches trip id (i.e., that the trip has stoptimes)
        if (!firstStopTime.trip_id.equals(trip_id) || !lastStopTime.trip_id.equals(trip_id)) {
            return Double.NaN;
        }

        double distance = getTripDistance(trip_id, straightLine);

        // trip time (in seconds)
        int time = lastStopTime.arrival_time - firstStopTime.departure_time;

        return distance / time; // meters per second
    }

    public Polygon getConvexHull() {
        if (this.convexHull == null) {
            synchronized (this) {
                List<Coordinate> coordinates = this.stops.values().stream().map(
                        stop -> new Coordinate(stop.stop_lon, stop.stop_lat)
                ).collect(Collectors.toList());
                Coordinate[] coords = coordinates.toArray(new Coordinate[coordinates.size()]);
                ConvexHull convexHull = new ConvexHull(coords, gf);
                this.convexHull = (Polygon) convexHull.getConvexHull();
            }
        }
        return this.convexHull;
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

    protected void finalize() throws IOException {
        close();
    }

    public void close () {
        db.close();
    }

    /** Thrown when we cannot interpolate stop times because the first or last stops do not have times */
    public class FirstAndLastStopsDoNotHaveTimes extends Exception {
        /** do nothing */
    }

    /** Create a GTFS feed in a temp file */
    public GTFSFeed () {
        // calls to this must be first operation in constructor - why, Java?
        this(DBMaker.newTempFileDB()
                .transactionDisable()
                .mmapFileEnable()
                .asyncWriteEnable()
                .deleteFilesAfterClose()
                .compressionEnable()
                // .cacheSize(1024 * 1024) this bloats memory consumption
                .make()); // TODO db.close();
    }

    /** Create a GTFS feed connected to a particular DB, which will be created if it does not exist. */
    public GTFSFeed (String dbFile) throws IOException, ExecutionException {
        this(constructDB(dbFile)); // TODO db.close();
    }

    private static DB constructDB(String dbFile) {
        DB db;
        try{
            DBMaker dbMaker = DBMaker.newFileDB(new File(dbFile));
            db = dbMaker
                    .transactionDisable()
                    .mmapFileEnable()
                    .asyncWriteEnable()
                    .compressionEnable()
//                     .cacheSize(1024 * 1024) this bloats memory consumption
                    .make();
            return db;
        } catch (ExecutionError | IOError | Exception e) {
            LOG.error("Could not construct db from file.", e);
            return null;
        }
    }

    private GTFSFeed (DB db) {
        this.db = db;

        agency = db.getTreeMap("agency");
        feedInfo = db.getTreeMap("feed_info");
        routes = db.getTreeMap("routes");
        trips = db.getTreeMap("trips");
        stop_times = db.getTreeMap("stop_times");
        frequencies = db.getTreeSet("frequencies");
        transfers = db.getTreeMap("transfers");
        stops = db.getTreeMap("stops");
        fares = db.getTreeMap("fares");
        services = db.getTreeMap("services");
        shape_points = db.getTreeMap("shape_points");
        translations = db.getTreeMap("translations");
        attributions = db.getTreeMap("attributions");
        calenders = db.getTreeMap("calenders");

        // Flex tables.
        bookingRules = db.getTreeMap("booking_rules");
        locationGroups = db.getTreeMap("location_groups");
        locationMetaData = db.getTreeMap("location_meta_data");
        locationShapes = db.getTreeMap("location_shapes");

        feedId = db.getAtomicString("feed_id").get();
        checksum = db.getAtomicLong("checksum").get();

        // use Java serialization because MapDB serialization is very slow with JTS as they have a lot of references.
        // nothing else contains JTS objects
        patterns = db.createTreeMap("patterns")
                .valueSerializer(Serializer.JAVA)
                .makeOrGet();

        tripPatternMap = db.getTreeMap("patternForTrip");

        errors = db.getTreeSet("errors");
    }

    /**
     * If any of the flex only tables contain data, the assumption is that this is a GTFS Flex feed. These tables must
     * be loaded before this can be referenced. At the moment {@link StopTime} references this and is loaded after the
     * check is made on these tables.
     */
    public boolean isGTFSFlexFeed() {
        return
            !bookingRules.isEmpty() ||
            !locationGroups.isEmpty() ||
            !locationMetaData.isEmpty() ||
            !locationShapes.isEmpty();
    }
}
