package com.conveyal.gtfs;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.zip.ZipFile;

/**
 * Cache for MapDBs holding GTFS feed, mirrored to S3.
 *
 * This uses a soft-values cache because (it is assumed) you do not want to have multiple copies of the same GTFS feed
 * in memory. When you are storing a reference to the original GTFS feed, it may be retrieved from the cache and held
 * by the caller for some finite amount of time. If, during that time, it is removed from the cache and requested again,
 * we would connect another GTFSFeed to the same mapdb, which seems like an ideal way to corrupt mapdbs. SoftReferences
 * prevent this as it cannot be removed if it is referenced elsewhere.
 */
public class GTFSCache {

    private static final Logger LOG = LoggerFactory.getLogger(GTFSCache.class);

    private static final String GTFS_EXTENSION = ".zip";
    private static final String DB_EXTENSION = ".db";
    private static final String DBP_EXTENSION = ".db.p";

    public final String bucket;

    public final File cacheDir;

    private static AmazonS3 s3 = null;
    private LoadingCache<String, GTFSFeed> cache;

    /** If bucket is null, work offline and do not use S3 */
    public GTFSCache (String awsRegion, String bucket, File cacheDir) {
        if (awsRegion == null || bucket == null) LOG.info("No AWS region/bucket specified; GTFS Cache will run locally");
        else {
            s3 = AmazonS3ClientBuilder.standard().withRegion(awsRegion).build();
            LOG.info("Using bucket {} for GTFS Cache", bucket);
        }

        this.bucket = bucket;

        this.cacheDir = cacheDir;

        RemovalListener<String, GTFSFeed> removalListener = removalNotification -> {
            try {
                LOG.info("Evicting feed {} from gtfs-cache and closing MapDB file. Reason: {}",
                        removalNotification.getKey(),
                        removalNotification.getCause());
                // Close DB to avoid leaking (off-heap allocated) memory for MapDB object cache, and MapDB corruption.
                removalNotification.getValue().close();
                // Delete local .zip file ONLY if using s3 (i.e. when 'bucket' has been set to something).
                // TODO elaborate on why we would want to do this.
                if (bucket != null) {
                    String id = removalNotification.getKey();
                    String[] extensions = {GTFS_EXTENSION}; // used to include ".db", ".db.p" as well.  See #119
                    // delete local cache files (including zip) when feed removed from cache
                    for (String type : extensions) {
                        File file = new File(cacheDir, id + type);
                        file.delete();
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("Exception while trying to evict GTFS MapDB from cache.", e);
            }
        };
        this.cache = (LoadingCache<String, GTFSFeed>) CacheBuilder.newBuilder()
                // we use SoftReferenced values because we have the constraint that we don't want more than one
                // copy of a particular GTFSFeed object around; that would mean multiple MapDBs are pointing at the same
                // file, which is bad.
                //.maximumSize(x) should be less sensitive to GC, though it interacts with instance cache size of MapDB
                // itself. But is MapDB instance cache on or off heap?
                .maximumSize(20)
                // .softValues()
                .removalListener(removalListener)
                .build(new CacheLoader() {
                    public GTFSFeed load(Object s) throws Exception {
                        // Thanks, java, for making me use a cast here. If I put generic arguments to new CacheLoader
                        // due to type erasure it can't be sure I'm using types correctly.
                        return retrieveAndProcessFeed((String) s);
                    }
                });
    }

    /**
     * Build the MapDB files for the given GTFS feed ZIP, with the specified ID, and return the MapDB (without
     * putting it in the cache). This ID is not the feed's self-declared feed ID, because we may load multiple
     * versions of the same feed.
     */
    public GTFSFeed put (String id, File feedFile) throws Exception {
        return put(id, feedFile, null);
    }

    /**
     * Build the MapDB files for the given GTFS feed ZIP, with the specified ID, and return the MapDB (without
     * putting it in the cache). The supplied function will generate a unique name for the feed (in case multiple
     * versions of the same feed are loaded), and is evaluated after the feed is already loaded so it can examine the
     * contents of the feed when generating the ID.
     */
    public GTFSFeed put (Function<GTFSFeed, String> idGenerator, File feedFile) throws Exception {
        return put(null, feedFile, idGenerator);
    }

    // TODO rename these methods. This does a lot more than a typical Map.put method so the name gets confusing.
    private GTFSFeed put (String id, File feedFile, Function<GTFSFeed, String> idGenerator) throws Exception {
        // generate temporary ID to name files
        String tempId = id != null ? id : UUID.randomUUID().toString();

        // read the feed
        String cleanTempId = cleanId(tempId);
        File dbFile = new File(cacheDir, cleanTempId + DB_EXTENSION);
        File movedFeedFile = new File(cacheDir, cleanTempId + GTFS_EXTENSION);

        // don't copy if we're loading from a locally-cached feed
        if (!feedFile.equals(movedFeedFile)) Files.copy(feedFile, movedFeedFile);

        GTFSFeed feed = new GTFSFeed(dbFile.getAbsolutePath());
        feed.loadFromFile(new ZipFile(movedFeedFile));
        feed.findPatterns();

        if (idGenerator != null) id = idGenerator.apply(feed);

        String cleanId = cleanId(id);

        // Close the DB and flush to disk before we start moving and copying files around.
        feed.close();

        if (idGenerator != null) {
            // This mess seems to be necessary to get around Windows file locks.
            File originalZip = new File(cacheDir, cleanTempId + GTFS_EXTENSION);
            File originalDb = new File(cacheDir, cleanTempId + DB_EXTENSION);
            File originalDbp = new File(cacheDir, cleanTempId + DBP_EXTENSION);
            Files.copy(originalZip,(new File(cacheDir, cleanId + GTFS_EXTENSION)));
            Files.copy(originalDb,(new File(cacheDir, cleanId + DB_EXTENSION)));
            Files.copy(originalDbp,(new File(cacheDir, cleanId + DBP_EXTENSION)));
            originalZip.delete();
            originalDb.delete();
            originalDbp.delete();
        }

        // Upload feed and MapDB files to S3 for long-term retrieval by workers and future backends.
        if (bucket != null) {
            LOG.info("Writing feed to S3 for long-term storage.");

            // write zip to s3 if not already there
            if (!s3.doesObjectExist(bucket, cleanId + GTFS_EXTENSION)) {
                s3.putObject(bucket, cleanId + GTFS_EXTENSION, feedFile);
                LOG.info("GTFS zip file written.");
            }
            else {
                LOG.info("Zip file already exists on s3.");
            }
            s3.putObject(bucket, cleanId + DB_EXTENSION, new File(cacheDir, cleanId + DB_EXTENSION));
            s3.putObject(bucket, cleanId + DBP_EXTENSION, new File(cacheDir, cleanId + DBP_EXTENSION));
            LOG.info("GTFS MapDB files written to S3.");
        }

        // Reopen the feed database so we can return it ready for use to the caller. Note that we do not add the feed
        // to the cache here. The returned feed is inserted automatically into the LoadingCache by the CacheLoader.
        feed = new GTFSFeed(new File(cacheDir, cleanId + DB_EXTENSION).getAbsolutePath());
        return feed;
    }

    public GTFSFeed get (String uniqueId) {
        try {
            return cache.get(uniqueId);
        } catch (ExecutionException e) {
            LOG.error("Error loading local MapDB.", e);
            deleteLocalDBFiles(uniqueId);
            return null;
        }
    }

    /** retrieve a feed from local cache or S3 */
    private GTFSFeed retrieveAndProcessFeed (String originalId) {

        // First try to load an already-existing GTFS MapDB cached locally
        String id = cleanId(originalId);
        File dbFile = new File(cacheDir, id + DB_EXTENSION);
        GTFSFeed feed;
        if (dbFile.exists()) {
            LOG.info("Processed GTFS was found cached locally");
            try {
                feed = new GTFSFeed(dbFile.getAbsolutePath());
                if (feed != null) {
                    return feed;
                }
            } catch (Exception e) {
                LOG.warn("Error loading local MapDB.", e);
                deleteLocalDBFiles(id);
            }
        }

        // Fallback - try to fetch an already-built GTFS MapDB from S3
        if (bucket != null) {
            try {
                LOG.info("Attempting to download cached GTFS MapDB.");
                S3Object db = s3.getObject(bucket, id + DB_EXTENSION);
                InputStream is = db.getObjectContent();
                FileOutputStream fos = new FileOutputStream(dbFile);
                ByteStreams.copy(is, fos);
                is.close();
                fos.close();

                S3Object dbp = s3.getObject(bucket, id + DBP_EXTENSION);
                InputStream isp = dbp.getObjectContent();
                FileOutputStream fosp = new FileOutputStream(new File(cacheDir, id + DBP_EXTENSION));
                ByteStreams.copy(isp, fosp);
                isp.close();
                fosp.close();

                LOG.info("Returning processed GTFS from S3");
                feed = new GTFSFeed(dbFile.getAbsolutePath());
                if (feed != null) {
                    return feed;
                }
            } catch (AmazonS3Exception e) {
                LOG.warn("DB file for key {} does not exist on S3.", id);
            } catch (ExecutionException | IOException e) {
                LOG.warn("Error retrieving MapDB from S3, will load from original GTFS.", e);
            }
        }

        // Fetching an already built MapDB from S3 was unsuccessful.
        // Instead, try to build a MapDB from the original GTFS ZIP in the local cache, and falling back to S3.
        File feedFile = new File(cacheDir, id + GTFS_EXTENSION);
        if (feedFile.exists()) {
            LOG.info("Loading feed from local cache directory...");
        }

        if (!feedFile.exists() && bucket != null) {
            LOG.info("Feed not found locally, downloading from S3.");
            try {
                S3Object gtfs = s3.getObject(bucket, id + GTFS_EXTENSION);
                InputStream is = gtfs.getObjectContent();
                FileOutputStream fos = new FileOutputStream(feedFile);
                ByteStreams.copy(is, fos);
                is.close();
                fos.close();
            } catch (Exception e) {
                LOG.error("Could not download feed at s3://{}/{}.", bucket, id);
                throw new RuntimeException(e);
            }
        }

        if (feedFile.exists()) {
            // TODO this will also re-upload the original feed ZIP to S3.
            try {
                return put(originalId, feedFile);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            LOG.warn("Feed {} not found locally", originalId);
            throw new NoSuchElementException(originalId);
        }
    }

    private void deleteLocalDBFiles(String id) {
        String[] extensions = {DB_EXTENSION, DBP_EXTENSION};
        // delete ONLY local cache db files
        for (String type : extensions) {
            File file = new File(cacheDir, id + type);
            file.delete();
        }
    }

    public static String cleanId(String id) {
        // replace all special characters with `-`, except for underscore `_`
        return id.replaceAll("[^A-Za-z0-9_]", "-");
    }
}
