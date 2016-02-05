package com.conveyal.gtfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GTFSMain {

    private static final Logger LOG = LoggerFactory.getLogger(GTFSMain.class);

    public static void main (String[] args) {

        if (args.length < 1) {
            System.out.printf("specify a GTFS feed to load.");
        }
        String inputFile = args[0];
            
        GTFSFeed feed = GTFSFeed.fromFile(inputFile);
        feed.findPatterns();

        if (args.length == 2)
            feed.toFile(args[1]);
        
        feed.db.close();
    }

}
