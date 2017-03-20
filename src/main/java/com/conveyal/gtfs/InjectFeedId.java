package com.conveyal.gtfs;

import com.conveyal.gtfs.model.FeedInfo;
import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

/**
 * Inject a feed ID into a GTFS file.
 */
public class InjectFeedId {
    private static final Logger LOG = LoggerFactory.getLogger(InjectFeedId.class);

    /** Inject Feed ID feedId into GTFS file file. If force is false, don't inject it if the file already has a feed ID */
    public static void inject (File infile, File outfile, String feedId, boolean force) throws IOException {
        GTFSFeed feed = new GTFSFeed();
        ZipFile zip = new ZipFile(infile);
        new FeedInfo.Loader(feed).loadTable(zip);

        FeedInfo feedInfo = feed.getFeedInfo();
        if (feedInfo == null) feedInfo = new FeedInfo();
        // NONE is a special keyword for
        if ("NONE".equals(feedInfo.feed_id) || force) {
            LOG.info("Injecting feed ID {} into file {}", feedId, infile);
            feedInfo.feed_id = feedId;
        } else {
            LOG.info("Feed {} already has feed ID {}, not replacing", infile, feedId);
        }

        feed.feedInfo.put("NONE", feedInfo);

        ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(outfile));
        new FeedInfo.Writer(feed).writeTable(zos);

        for (Enumeration<? extends  ZipEntry> entries = zip.entries(); entries.hasMoreElements();) {
            ZipEntry e = entries.nextElement();
            if (!"feed_info.txt".equals(e.getName())) { // we injected a new feed_info.txt, so skip copying that file
                InputStream is = zip.getInputStream(e);
                zos.putNextEntry(new ZipEntry(e.getName()));
                ByteStreams.copy(is, zos);
            }
        }

        zos.close();
    }
}
