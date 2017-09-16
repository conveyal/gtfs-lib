package com.conveyal.gtfs.stats;

import com.conveyal.gtfs.GTFSFeed;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class FeedStatsTest {

    /*****************************************
     * getAverageWeekdayRevenueTime
     *****************************************/

    @Test
    public void canCalculateWithEmptyFeed() {
        GTFSFeed feed = new GTFSFeed();
        FeedStats feedStats = new FeedStats(feed);
        assertThat(feedStats.getAverageWeekdayRevenueTime(), equalTo(0L));
    }
}
