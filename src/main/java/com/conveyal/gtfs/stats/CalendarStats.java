package com.conveyal.gtfs.stats;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.model.Service;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by landon on 9/2/16.
 */
public class CalendarStats {
    private GTFSFeed feed = null;
    private FeedStats stats = null;

//    public CalendarStats (GTFSFeed f) {
//        feed = f;
//        stats = new FeedStats(feed);
//    }

//    public Set<String> getServiceIdsForDates (LocalDate from, LocalDate to) {
//        long days = ChronoUnit.DAYS.between(from, to);
//
//        return feed.services.values().stream()
//                .filter(s -> {
//                    for (int i = 0; i < days; i++) {
//                        LocalDate date = from.plusDays(i);
//                        if (s.activeOn(date)) {
//                            return true;
//                        }
//                    }
//                    return false;
//                })
//                .map(s -> s.service_id)
//                .collect(Collectors.toSet());
//    }
}
