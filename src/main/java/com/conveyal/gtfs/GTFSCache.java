package com.conveyal.gtfs;

import java.io.File;

/**
 * Simple cache that just fetches and stores GTFS feeds directly, and does not transform them in any way.
 */
public class GTFSCache extends BaseGTFSCache<GTFSFeed> {
    public GTFSCache(String bucket, File cacheDir) {
        super(bucket, cacheDir);
    }

    public GTFSCache(String bucket, String bucketFolder, File cacheDir) {
        super(bucket, bucketFolder, cacheDir);
    }
    
    @Override
    protected GTFSFeed processFeed(GTFSFeed feed) {
        return feed;
    }
}
