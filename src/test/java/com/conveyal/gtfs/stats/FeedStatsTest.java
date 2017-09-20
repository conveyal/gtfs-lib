package com.conveyal.gtfs.stats;

import com.conveyal.gtfs.GTFSFeed;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Unit tests to verify functionality of items with the FeedStats class.
 */
public class FeedStatsTest {

    /*****************************************
     * getAverageWeekdayRevenueTime
     *****************************************/

    /**
     * This test makes sure that a FeedStats is able to run the getAverageWeekdayRevenueTime method
     * when given an empty GTFSFeed instance.
     */
    @Test
    public void canCalculateWithEmptyFeed() {
        GTFSFeed feed = new GTFSFeed();
        FeedStats feedStats = new FeedStats(feed);
        assertThat(feedStats.getAverageWeekdayRevenueTime(), equalTo(0L));
    }
}
