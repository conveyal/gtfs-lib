package com.conveyal.gtfs;

import com.conveyal.gtfs.validator.service.impl.FeedStats;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.zip.ZipFile;

public class GTFSMain {

    private static final Logger LOG = LoggerFactory.getLogger(GTFSMain.class);

    public static void main (String[] args) throws Exception {

        if (args.length < 1) {
            System.out.printf("specify a GTFS feed to load.");
        }
        File tempFile = File.createTempFile("gtfs", ".db");

        GTFSFeed feed = new GTFSFeed(tempFile.getAbsolutePath());
        feed.loadFromFile(new ZipFile(args[0]));

        feed.findPatterns();
        
        feed.close();

        LOG.info("reopening feed");

        // re-open
        GTFSFeed reconnected = new GTFSFeed(tempFile.getAbsolutePath());

        LOG.info("Connected to already loaded feed");

        LOG.info("  {} routes", reconnected.routes.size());
        LOG.info("  {} trips", reconnected.trips.size());
        LOG.info("  {} stop times", reconnected.stop_times.size());
    }

}
