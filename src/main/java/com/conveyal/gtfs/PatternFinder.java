package com.conveyal.gtfs;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.model.Area;
import com.conveyal.gtfs.model.Location;
import com.conveyal.gtfs.model.Pattern;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopArea;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static com.conveyal.gtfs.util.Util.human;

/**
 * This abstracts out the logic for finding stop sequences ("journey patterns" in Transmodel parlance) based on trips.
 * Placing this logic in a separate class allows us to use it on GTFS data from multiple sources.
 * Our two specific use cases are finding patterns in stop_times that have already been loaded into an RDBMS, and
 * finding patterns while loading Java objects directly into a MapDB database.
 *
 * Created by abyrd on 2017-10-08
 */
public class PatternFinder {

    private static final Logger LOG = LoggerFactory.getLogger(PatternFinder.class);

    // A multi-map that groups trips together by their sequence of stops
    private Multimap<TripPatternKey, Trip> tripsForPattern = LinkedHashMultimap.create();

    private int nTripsProcessed = 0;

    public void processTrip(Trip trip, Iterable<StopTime> orderedStopTimes) {
        if (++nTripsProcessed % 100000 == 0) {
            LOG.info("trip {}", human(nTripsProcessed));
        }
        // No need to scope the route ID here, patterns are built within the context of a single feed.
        // Create a key that might already be in the map (by semantic equality)
        TripPatternKey key = new TripPatternKey(trip.route_id);
        for (StopTime st : orderedStopTimes) {
            key.addStopTime(st);
        }
        // Add the current trip to the map, possibly extending an existing list of trips on this pattern.
        tripsForPattern.put(key, trip);
    }

    /**
     * Once all trips have been processed, call this method to produce the final Pattern objects representing all the
     * unique sequences of stops encountered. Returns map of patterns to their keys so that downstream functions can
     * make use of trip pattern keys for constructing pattern stops or other derivative objects.
     */
    public Map<TripPatternKey, Pattern> createPatternObjects(
        Map<String, Stop> stopById,
        Map<String, Location> locationById,
        Map<String, StopArea> stopAreaById,
        Map<String, Area> areaById,
        SQLErrorStorage errorStorage
    ) {
        // Make pattern ID one-based to avoid any JS type confusion between an ID of zero vs. null value.
        int nextPatternId = 1;
        // Create an in-memory list of Patterns because we will later rename them before inserting them into storage.
        // Use a LinkedHashMap so we can retrieve the entrySets later in the order of insertion.
        Map<TripPatternKey, Pattern> patterns = new LinkedHashMap<>();
        // TODO assign patterns sequential small integer IDs (may include route)
        for (TripPatternKey key : tripsForPattern.keySet()) {
            Collection<Trip> trips = tripsForPattern.get(key);
            Pattern pattern = new Pattern(key.stops, trips, null);
            // Overwrite long UUID with sequential integer pattern ID
            pattern.pattern_id = Integer.toString(nextPatternId++);
            // FIXME: Should associated shapes be a single entry?
            pattern.associatedShapes = new HashSet<>();
            trips.stream().forEach(trip -> pattern.associatedShapes.add(trip.shape_id));
            if (pattern.associatedShapes.size() > 1 && errorStorage != null) {
                // Store an error if there is more than one shape per pattern. Note: error storage is null if called via
                // MapDB implementation.
                // TODO: Should shape ID be added to trip pattern key?
                errorStorage.storeError(NewGTFSError.forEntity(
                        pattern,
                        NewGTFSErrorType.MULTIPLE_SHAPES_FOR_PATTERN)
                            .setBadValue(pattern.associatedShapes.toString()));
            }
            patterns.put(key, pattern);
        }
        // Name patterns before storing in SQL database.
        renamePatterns(patterns.values(), stopById, locationById, stopAreaById, areaById);
        LOG.info("Total patterns: {}", tripsForPattern.keySet().size());
        return patterns;
    }

    /**
     * Destructively rename the supplied collection of patterns. This process requires access to all stops, locations
     * and stop areas in the feed. Some validators already cache a map of all the stops. There's probably a
     * cleaner way to do this.
     */
    public static void renamePatterns(
        Collection<Pattern> patterns,
        Map<String, Stop> stopById,
        Map<String, Location> locationById,
        Map<String, StopArea> stopAreaById,
        Map<String, Area> areaById
    ) {
        LOG.info("Generating unique names for patterns");

        Map<String, PatternNamingInfo> namingInfoForRoute = new HashMap<>();

        for (Pattern pattern : patterns) {
            if (pattern.associatedTrips.isEmpty() || pattern.orderedStops.isEmpty()) continue;

            // Each pattern within a route has a unique name (within that route, not across the entire feed)

            PatternNamingInfo namingInfo = namingInfoForRoute.get(pattern.route_id);
            if (namingInfo == null) {
                namingInfo = new PatternNamingInfo();
                namingInfoForRoute.put(pattern.route_id, namingInfo);
            }

            // Pattern names are built using stop names rather than stop IDs.
            // Stop names, unlike IDs, are not guaranteed to be unique.
            // Therefore we must track used names carefully to avoid duplicates.

            String fromName = getTerminusName(pattern, stopById, locationById, stopAreaById, areaById, true);
            String toName = getTerminusName(pattern, stopById, locationById, stopAreaById, areaById, false);

            namingInfo.fromStops.put(fromName, pattern);
            namingInfo.toStops.put(toName, pattern);

            for (String stopId : pattern.orderedStops) {
                Stop stop = stopById.get(stopId);
                // If the stop doesn't exist, it's probably a location or stop area and can be ignored for renaming.
                if (stop == null || fromName.equals(stop.stop_name) || toName.equals(stop.stop_name)) continue;
                namingInfo.vias.put(stop.stop_name, pattern);
            }
            namingInfo.patternsOnRoute.add(pattern);
        }

        // name the patterns on each route
        for (PatternNamingInfo info : namingInfoForRoute.values()) {
            for (Pattern pattern : info.patternsOnRoute) {
                pattern.name = null; // clear this now so we don't get confused later on
                String fromName = getTerminusName(pattern, stopById, locationById, stopAreaById, areaById, true);
                String toName = getTerminusName(pattern, stopById, locationById, stopAreaById, areaById, false);

                // check if combination from, to is unique
                Set<Pattern> intersection = new HashSet<>(info.fromStops.get(fromName));
                intersection.retainAll(info.toStops.get(toName));

                if (intersection.size() == 1) {
                    pattern.name = String.format(Locale.US, "from %s to %s", fromName, toName);
                    continue;
                }

                // check for unique via stop
                pattern.orderedStops.stream().map(
                    uniqueEntityId -> getStopType(uniqueEntityId, stopById, locationById, stopAreaById)
                ).forEach(entity -> {
                    Set<Pattern> viaIntersection = new HashSet<>(intersection);
                    String stopName = getStopName(entity, areaById);
                    viaIntersection.retainAll(info.vias.get(stopName));

                    if (viaIntersection.size() == 1) {
                        pattern.name = String.format(Locale.US, "from %s to %s via %s", fromName, toName, stopName);
                    }
                });

                if (pattern.name == null) {
                    // no unique via, one pattern is subset of other.
                    if (intersection.size() == 2) {
                        Iterator<Pattern> it = intersection.iterator();
                        Pattern p0 = it.next();
                        Pattern p1 = it.next();
                        if (p0.orderedStops.size() > p1.orderedStops.size()) {
                            p1.name = String.format(Locale.US, "from %s to %s express", fromName, toName);
                            p0.name = String.format(Locale.US, "from %s to %s local", fromName, toName);
                        } else if (p1.orderedStops.size() > p0.orderedStops.size()){
                            p0.name = String.format(Locale.US, "from %s to %s express", fromName, toName);
                            p1.name = String.format(Locale.US, "from %s to %s local", fromName, toName);
                        }
                    }
                }

                if (pattern.name == null) {
                    // give up
                    pattern.name = String.format(Locale.US, "from %s to %s like trip %s", fromName, toName, pattern.associatedTrips.get(0));
                }
            }

            // attach a stop and trip count to each
            for (Pattern pattern : info.patternsOnRoute) {
                pattern.name = String.format(Locale.US, "%s stops %s (%s trips)",
                        pattern.orderedStops.size(), pattern.name, pattern.associatedTrips.size());
            }
        }
    }

    /**
     * Using the 'unique stop id' return the object it actually relates to. Under flex, a stop id can either be a stop,
     * location or stop area, this method decides which.
     */
    private static Object getStopType(
        String uniqueEntityId,
        Map<String, Stop> stopById,
        Map<String, Location> locationById,
        Map<String, StopArea> stopAreaById
    ) {
        if (stopById.get(uniqueEntityId) != null) {
            return stopById.get(uniqueEntityId);
        } else if (locationById.get(uniqueEntityId) != null) {
            return locationById.get(uniqueEntityId);
        } else if (stopAreaById.get(uniqueEntityId) != null) {
            return stopAreaById.get(uniqueEntityId);
        } else {
            return null;
        }
    }

    /**
     * Extract the 'stop name' from either a stop, location or area (via stop area) depending on the entity type.
     */
    private static String getStopName(Object entity, Map<String, Area> areaById) {
        if (entity != null) {
            if (entity instanceof Stop) {
                return ((Stop) entity).stop_name;
            } else if (entity instanceof Location) {
                return ((Location) entity).stop_name;
            } else if (entity instanceof StopArea) {
                StopArea stopArea = (StopArea) entity;
                Area area = areaById.get(stopArea.area_id);
                return (area != null) ? area.area_name : "stopNameUnknown";
            }
        }
        return "stopNameUnknown";
    }

    /**
     * Return either the 'from' or 'to' terminus name. Check the list of stops first, if there is no match, then check
     * the locations or areas (via stop areas). If neither provide a match return a default value.
     */
    private static String getTerminusName(
        Pattern pattern,
        Map<String, Stop> stopById,
        Map<String, Location> locationById,
        Map<String, StopArea> stopAreaById,
        Map<String, Area> areaById,
        boolean isFrom
    ) {
        int id = isFrom ? 0 : pattern.orderedStops.size() - 1;
        String haltId = pattern.orderedStops.get(id);
        if (stopById.containsKey(haltId)) {
            return stopById.get(haltId).stop_name;
        } else if (locationById.containsKey(haltId)) {
            return locationById.get(haltId).stop_name;
        } else if (stopAreaById.containsKey(haltId)) {
            return areaById.get(haltId).area_name;
        }
        return isFrom ? "fromTerminusNameUnknown" : "toTerminusNameUnknown";
    }

    /**
     * Holds information about all pattern names on a particular route,
     * modeled on https://github.com/opentripplanner/OpenTripPlanner/blob/master/src/main/java/org/opentripplanner/routing/edgetype/TripPattern.java#L379
     */
    private static class PatternNamingInfo {
        // These are all maps from ?
        // FIXME For type safety and clarity maybe we should have a parameterized ID type, i.e. EntityId<Stop> stopId.
        Multimap<String, Pattern> fromStops = HashMultimap.create();
        Multimap<String, Pattern> toStops = HashMultimap.create();
        Multimap<String, Pattern> vias = HashMultimap.create();
        List<Pattern> patternsOnRoute = new ArrayList<>();
    }

}
