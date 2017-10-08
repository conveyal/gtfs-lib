package com.conveyal.gtfs;

import com.conveyal.gtfs.model.Pattern;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
    private Multimap<TripPatternKey, Trip> tripsForPattern = HashMultimap.create();

    private int nTripsProessed = 0;

    /**
     *  Bin all trips by the sequence of stops they visit.
     * @return A map from a list of stop IDs to a list of Trip IDs that visit those stops in that sequence.
     */

//    public void findPatterns(Feed feed) {
//
//        for (Trip trip : trips) {
//        }
//        feed.patterns.stream().forEach(p -> {
//            feed.patterns.put(p.pattern_id, p);
//            p.associatedTrips.stream().forEach(t -> feed.tripPatternMap.put(t, p.pattern_id));
//        });
//
//    }

    public void processTrip (Trip trip, List<StopTime> orderedStopTimes) {
        if (++nTripsProessed % 100000 == 0) {
            LOG.info("trip {}", human(nTripsProessed));
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
     * unique sequences of stops encountered.
     */
    public List<Pattern> createPatternObjects () {
        // Create an in-memory list of Patterns because we will later rename them before inserting them into storage.
        List<Pattern> patterns = new ArrayList<>();
        tripsForPattern.asMap().forEach((key, trips) -> patterns.add(new Pattern(key.stops, trips, null)));
        // Attempt to assign more human-readable names to all these patterns.
        renamePatterns(patterns);
        LOG.info("Total patterns: {}", tripsForPattern.keySet().size());
        return patterns;
    }

    /** destructively rename passed in patterns */
    private void renamePatterns(Collection<Pattern> patterns) {
        LOG.info("Generating unique names for patterns");

        Map<String, PatternNamingInfo> namingInfoForRoute = new HashMap<>();

        for (Pattern pattern : patterns) {
            if (pattern.associatedTrips.isEmpty() || pattern.orderedStops.isEmpty()) continue;

            Trip trip = trips.get(pattern.associatedTrips.get(0));

            // TODO this assumes there is only one route associated with a pattern
            String route = trip.route_id;

            // names are unique at the route level
            if (!namingInfoForRoute.containsKey(route)) namingInfoForRoute.put(route, new PatternNamingInfo());
            PatternNamingInfo namingInfo = namingInfoForRoute.get(route);

            if (trip.trip_headsign != null)
                namingInfo.headsigns.put(trip.trip_headsign, pattern);

            // use stop names not stop IDs as stops may have duplicate names and we want unique pattern names
            String fromName = stops.get(pattern.orderedStops.get(0)).stop_name;
            String toName = stops.get(pattern.orderedStops.get(pattern.orderedStops.size() - 1)).stop_name;

            namingInfo.fromStops.put(fromName, pattern);
            namingInfo.toStops.put(toName, pattern);

            pattern.orderedStops.stream().map(stops::get).forEach(stop -> {
                if (fromName.equals(stop.stop_name) || toName.equals(stop.stop_name)) return;

                namingInfo.vias.put(stop.stop_name, pattern);
            });

            namingInfo.patternsOnRoute.add(pattern);
        }

        // name the patterns on each route
        for (PatternNamingInfo info : namingInfoForRoute.values()) {
            for (Pattern pattern : info.patternsOnRoute) {
                pattern.name = null; // clear this now so we don't get confused later on

                String headsign = trips.get(pattern.associatedTrips.get(0)).trip_headsign;

                String fromName = stops.get(pattern.orderedStops.get(0)).stop_name;
                String toName = stops.get(pattern.orderedStops.get(pattern.orderedStops.size() - 1)).stop_name;

                // check if combination from, to is unique
                Set<Pattern> intersection = new HashSet<>(info.fromStops.get(fromName));
                intersection.retainAll(info.toStops.get(toName));

                if (intersection.size() == 1) {
                    pattern.name = String.format(Locale.US, "from %s to %s", fromName, toName);
                    continue;
                }

                // check for unique via stop
                pattern.orderedStops.stream().map(stops::get).forEach(stop -> {
                    Set<Pattern> viaIntersection = new HashSet<>(intersection);
                    viaIntersection.retainAll(info.vias.get(stop.stop_name));

                    if (viaIntersection.size() == 1) {
                        pattern.name = String.format(Locale.US, "from %s to %s via %s", fromName, toName, stop.stop_name);
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
     * Holds information about all pattern names on a particular route,
     * modeled on https://github.com/opentripplanner/OpenTripPlanner/blob/master/src/main/java/org/opentripplanner/routing/edgetype/TripPattern.java#L379
     */
    private static class PatternNamingInfo {
        // These are all maps from ?
        // FIXME For type safety and clarity maybe we should have a parameterized ID type, i.e. EntityId<Stop> stopId.
        Multimap<String, Pattern> headsigns = HashMultimap.create();
        Multimap<String, Pattern> fromStops = HashMultimap.create();
        Multimap<String, Pattern> toStops = HashMultimap.create();
        Multimap<String, Pattern> vias = HashMultimap.create();
        List<Pattern> patternsOnRoute = new ArrayList<>();
    }

}
