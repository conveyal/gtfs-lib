package com.conveyal.gtfs;

import java.io.File;

/**
 * Simple cache that just fetches and stores GTFS feeds directly, and does not transform them in any way.
 */
public class GTFSCache extends BaseGTFSCache<GTFSFeed> {

    public GTFSCache(String region, String bucket, File cacheDir) {
        super(region, bucket, cacheDir);
    }
    
    @Override
    protected GTFSFeed processFeed(GTFSFeed feed) {
        return feed;
    }

    @Override
    public GTFSFeed getFeed (String id) {
        return this.get(id);
    }

}
