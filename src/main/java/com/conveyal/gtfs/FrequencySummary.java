package com.conveyal.gtfs;

import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.stats.FeedStats;
import com.conveyal.gtfs.stats.RouteStats;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Summarize frequencies, designed for use in the KCMO transportation system.
 */
public class FrequencySummary {
    private static final LocalTime[] windows = new LocalTime[] {
            LocalTime.of(6, 0),
            LocalTime.of(9, 0),
            LocalTime.of(16, 0),
            LocalTime.of(18, 0),
            LocalTime.of(22, 0)
    };

    private static final LocalDate date = LocalDate.of(2016, 8, 30);

    public static void main(String... args) throws IOException {
        GTFSFeed feed = GTFSFeed.fromFile(args[0]);
        feed.findPatterns();
        FeedStats fs = new FeedStats(feed);
        RouteStats stats = new RouteStats(feed, fs);

        FileWriter writer = new FileWriter(new File(args[1]));

        writer.write("Route,AM Peak Frequency,Midday Frequency,PM Peak Frequency,Evening Frequency,AM Peak Speed,Midday Speed,PM Peak Speed,Evening Speed,Trips/Day,First trip,Last trip\n");

        for (String route_id : feed.routes.keySet()) {
            for (int direction_id : new int[] { 0, 1 }) {
                StringBuilder freq = new StringBuilder();
                StringBuilder speed = new StringBuilder();

                Route r = feed.routes.get(route_id);

                freq.append(r.route_short_name);
                freq.append(" ");
                freq.append(r.route_long_name);

                // guess at the direction name by finding longest pattern
                List<String> stops = feed.patterns.values().stream()
                        .filter(p -> p.route_id.equals(route_id) && feed.trips.get(p.associatedTrips.get(0)).direction_id == direction_id)
                        .sorted((p1, p2) -> p1.orderedStops.size() - p2.orderedStops.size())
                        .findFirst()
                        .get()
                        .orderedStops;

                Stop lastStop = feed.stops.get(stops.get(stops.size() - 1));

                freq.append(" towards ");
                freq.append(lastStop.stop_name);

                freq.append(",");

                for (int window = 0; window < windows.length - 1; window++) {
                    int headwaySecs = stats.getHeadwayForRouteDirection(route_id, direction_id, date, windows[window], windows[window + 1]);
                    freq.append(headwaySecs > 0 ? Math.round(headwaySecs / 60 ) : "-");
                    freq.append(",");

                    double speedMs = stats.getSpeedForRouteDirection(route_id, direction_id, date, windows[window], windows[window + 1]);
                    speed.append(Double.isNaN(speedMs) ? "-" : Math.round(speedMs * 3600 / 1609.0 ));
                    speed.append(",");
                }

                speed.append(stats.getTripsPerDateOfService(route_id).get(date).stream().filter(t -> t.direction_id == direction_id).count());
                speed.append(",");
                speed.append(stats.getStartTimeForRouteDirection(route_id, direction_id, date).format(DateTimeFormatter.ofPattern("K:mm a")));
                speed.append(",");
                speed.append(stats.getEndTimeForRouteDirection(route_id, direction_id, date).format(DateTimeFormatter.ofPattern("K:mm a")));

                writer.write(freq.toString());
                writer.write(speed.toString());
                writer.write("\n");
            }
        }

        writer.flush();
        writer.close();
    }
}
