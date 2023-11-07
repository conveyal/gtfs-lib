package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.loader.Table;
import com.google.common.base.Joiner;
import org.locationtech.jts.geom.LineString;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a collection of trips that all visit the same stops in the same sequence.
 */
public class Pattern extends Entity {
    public static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(Pattern.class);

    @Override
    public String getId () {
        return pattern_id;
    }

    // A unique ID for this journey pattern / stop pattern
    public String pattern_id;

    // The segment of the pattern's geometry (which is always a LineString) on which each stop in the sequence falls.
    public int[] segmentIndex;

    // The percentage in [0..1] along the line segment at which each stop in the sequence falls.
    public double[] segmentFraction;

    public List<String> orderedStops;
    // TODO: change list of trips to set
    public List<String> associatedTrips;
    // TODO: add set of shapes
    public Set<String> associatedShapes;
    public LineString geometry;
    public String name;
    public String route_id;
    public int direction_id = INT_MISSING;
    public int use_frequency = INT_MISSING;
    public String shape_id;
    public static Joiner joiner = Joiner.on("-").skipNulls();
    public String feed_id;

    // TODO: Should a Pattern be generated for a single trip or a set of trips that share the same ordered stop list?

    /**
     *
     * @param orderedStops
     * @param trips the first trip will serve as an exemplar for all the others.
     * @param patternGeometry
     */
    public Pattern (List<String> orderedStops, Collection<Trip> trips, LineString patternGeometry){

        // Temporarily make a random ID for the pattern, which might be overwritten in a later step ?
        this.pattern_id = UUID.randomUUID().toString();

        // Assign ordered list of stop IDs to be the key of this pattern.
        // FIXME what about pickup / dropoff type?
        this.orderedStops = orderedStops;

        // Save the string IDs of the trips on this pattern.
        this.associatedTrips = trips.stream().map(t -> t.trip_id).collect(Collectors.toList());

        // In theory all trips could take different paths and be on different routes.
        // Here we're using only the first one as an exemplar.
        String trip_id = associatedTrips.get(0);

        Trip exemplarTrip = trips.iterator().next();
        this.geometry = patternGeometry;

        // feed.getTripGeometry(exemplarTrip.trip_id);

        // Patterns have one and only one route.
        // FIXME are we certain we're only passing in trips on one route? or are we losing information here?
        this.route_id = exemplarTrip.route_id;
        this.direction_id = exemplarTrip.direction_id;

        // A name is assigned to this pattern based on the headsign, short name, direction ID or stop IDs.
        // This is not at all guaranteed to be unique, it's just to help identify the pattern.
        if (exemplarTrip.trip_headsign != null){
            name = exemplarTrip.trip_headsign;
        }
        else if (exemplarTrip.trip_short_name != null) {
            name = exemplarTrip.trip_short_name;
        }
        else if (exemplarTrip.direction_id >= 0){
            name = String.valueOf(exemplarTrip.direction_id);
        }
        else{
            name = joiner.join(orderedStops);
        }

        // TODO: Implement segmentIndex using JTS to segment out LineString by stops.

        // TODO: Implement segmentFraction using JTS to segment out LineString by stops.

    }

    public Pattern () {}

    public static class Loader extends Entity.Loader<Pattern> {

        public Loader(GTFSFeed feed) {
            super(feed, Table.getTableFileName("patterns"));
        }

        @Override
        protected boolean isRequired() {
            return false;
        }

        @Override
        public void loadOneRow() throws IOException {
            Pattern pattern = new Pattern();
            pattern.id = row + 1; // offset line number by 1 to account for 0-based row index
            pattern.pattern_id = getStringField("pattern_id", true);
            pattern.route_id = getStringField("route_id", true);
            pattern.name = getStringField("name", false);
            pattern.direction_id = getIntField("direction_id", false, 0, 1);
            pattern.use_frequency = getIntField("use_frequency", false, 0, 1);
            pattern.shape_id = getStringField("shape_id", false);
            pattern.feed = feed;
            pattern.feed_id = feed.feedId;
            // Attempting to put a null key or value will cause an NPE in BTreeMap.
            if (pattern.pattern_id != null) feed.patterns.put(pattern.pattern_id, pattern);
        }

    }

    public static class Writer extends Entity.Writer<Pattern> {
        public Writer (GTFSFeed feed) {
            super(feed, Table.getTableFileName("patterns"));
        }

        @Override
        protected void writeHeaders() throws IOException {
            writer.writeRecord(new String[] {"pattern_id", "route_id", "name", "direction_id", "use_frequency", "shape_id"});
        }

        @Override
        protected void writeOneRow(Pattern pattern) throws IOException {
            writeStringField(pattern.pattern_id);
            writeStringField(pattern.route_id);
            writeStringField(pattern.name);
            writeIntField(pattern.direction_id);
            writeIntField(pattern.use_frequency);
            writeStringField(pattern.shape_id);
            endRecord();
        }

        @Override
        protected Iterator<Pattern> iterator() {
            return feed.patterns.values().iterator();
        }
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#PATTERNS}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, pattern_id);
        statement.setString(oneBasedIndex++, route_id);
        statement.setString(oneBasedIndex++, name);
        // Editor-specific fields.
        setIntParameter(statement, oneBasedIndex++, direction_id);
        // Note: pattern#use_frequency is set in JdbcGtfsSnapshotter#populateDefaultEditorValues.
        setIntParameter(statement, oneBasedIndex++, INT_MISSING);
        if (associatedShapes != null) {
            statement.setString(oneBasedIndex, associatedShapes.iterator().next());
        } else {
            statement.setString(oneBasedIndex, "");
        }
    }
}
