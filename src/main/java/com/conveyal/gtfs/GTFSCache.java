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
import java.util.concurrent.ExecutionException;

/**
 * Cache of disk-backed (MapDB) GTFS feeds, falling back on long-term storage on S3.
 *
 * This uses a soft-values cache because (it is assumed) you do not want to have multiple copies of the same GTFS feed
 * in memory. When you are storing a reference to the original GTFS feed, it may be retrieved from the cache and held
 * by the caller for some finite amount of time. If, during that time, it is removed from the cache and requested again,
 * we would connect another GTFSFeed to the same mapdb, which seems like an ideal way to corrupt mapdbs. SoftReferences
 * prevent this as it cannot be removed if it is referenced elsewhere.
 */
public class GTFSCache {
    private static final Logger LOG = LoggerFactory.getLogger(GTFSCache.class);

    public final String bucket;

    public final File cacheDir;

    private static AmazonS3 s3 = null;

    private LoadingCache<String, GTFSFeed> cache;

    /** If bucket is null, work offline and do not use S3 */
    // TODO remove bucketFolder
    public GTFSCache (String awsRegion, String bucket, String bucketFolder, File cacheDir) {
        if (awsRegion == null || bucket == null) {
            LOG.info("No AWS region/bucket specified; GTFS Cache will run locally");
        } else {
            s3 = AmazonS3ClientBuilder.standard().withRegion(awsRegion).build();
            LOG.info("Using bucket {} for GTFS Cache", bucket);
        }

        this.bucket = bucket;
        this.cacheDir = cacheDir;

        CacheLoader<String, GTFSFeed> cacheLoader = new CacheLoader<>() {
            public GTFSFeed load (String s) {
                return retrieveAndProcessFeed(s);
            }
        };
        RemovalListener<String, GTFSFeed> removalListener = removalNotification -> {
            try {
                LOG.info("Evicting feed {} from gtfs-cache and closing MapDB file. Reason: {}",
                        removalNotification.getKey(),
                        removalNotification.getCause());
                // Close DB to avoid leaking (off-heap allocated) memory for MapDB object cache, and MapDB corruption.
                removalNotification.getValue().close();
                // Delete local .zip file ONLY if using s3 (i.e. when 'bucket' has been set to something).
                // TODO elaborate on why we would want to do this. Maybe just remove this code and use large EBS volume.
                if (bucket != null) {
                    String id = removalNotification.getKey();
                    String[] extensions = {".zip"}; // used to include ".db", ".db.p" as well.  See #119
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
        this.cache = CacheBuilder.newBuilder()
                // We used SoftReferenced values because we have the constraint that we don't want more than one
                // copy of a particular GTFSFeed object around; that would mean multiple MapDBs are pointing at the same
                // file, which is bad.
                //.maximumSize(x) should be less sensitive to GC, though it interacts with instance cache size of MapDB
                // itself. But is MapDB instance cache on or off heap?
                .maximumSize(20)
                // .softValues()
                .removalListener(removalListener)
                .build(cacheLoader);
    }

    /**
     * Build the MapDB files for the given GTFS feed ZIP, with the specified ID, and return the MapDB (without
     * putting it in the cache). This ID is not the feed's self-declared feed ID, because we may load multiple
     * versions of the same feed.
     *
     * TODO explain overwriting behavior when files already exist
     * @param sourceFile this file will be moved into the cache.
     */
    public GTFSFeed buildFeed (String id, File sourceFile) throws Exception {

        // Throw an exception if the supplied ID is not a sequence of alphanumeric characters and underscores.
        validateId(id);

        // Derive file names and File objects from the supplied ID.
        // TODO check that the file doesn't already exist? Or at least warn when overwriting?
        String zipName = id + ".zip";
        String dbName = id + ".v2.db";
        String dbpName = id + ".v2.db.p";
        File zipFile = new File(cacheDir, zipName);
        File dbFile = new File(cacheDir, dbName);
        File dbpFile = new File(cacheDir, dbpName);

        // Move the file into the local cache unless we're loading from a feed that is already in that local cache.
        // We used to copy, should we maintain that behavior?
        if (!sourceFile.equals(zipFile)) {
            Files.move(sourceFile, zipFile);
        }

        // Load the GTFS data into the GTFSFeed MapDB tables.
        GTFSFeed feed = new GTFSFeed(dbFile.getAbsolutePath());
        feed.loadFromFile(zipFile, id);
        feed.findPatterns();
        feed.close();

        // Upload original feed zip and MapDB files to S3 for long-term retrieval by workers and future deployments.
        if (bucket != null) {
            // Only upload the ZIP if it's not already on S3. We might be rebuilding MapDB files from an existing zip.
            if (!s3.doesObjectExist(bucket, zipName)) {
                LOG.info("Uploading GTFS feed and MapDB files to S3 for long-term storage...");
                s3.putObject(bucket, zipName, zipFile);
                LOG.info("GTFS ZIP file uploaded to S3.");
            } else {
                LOG.info("ZIP file already exists on S3, not uploading.");
            }
            // Always upload the MapDB files since calling this method is a request for them to be (re)built.
            s3.putObject(bucket, dbName, dbFile);
            s3.putObject(bucket, dbpName, dbpFile);
            LOG.info("MapDB files uploaded to S3.");
        }

        // After uploading, reconnect to the feed database so we can return a useable object.
        // Note that we do NOT put the feed into the cache here, the LoadingCache mechanism automatically handles that.
        feed = new GTFSFeed(new File(cacheDir, dbName).getAbsolutePath());
        return feed;
    }

    public GTFSFeed get (String id) {
        try {
            return cache.get(id);
        } catch (ExecutionException e) {
            LOG.error("Error loading local MapDB.", e);
            deleteLocalDBFiles(id);
            return null;
        }
    }

    /** Retrieve a feed from local cache or S3. */
    private GTFSFeed retrieveAndProcessFeed (String id) {

        // Throw an exception if the supplied ID is not a sequence of alphanumeric characters and underscores.
        validateId(id);

        // Derive file names and File objects from the supplied ID.
        // TODO check that the file doesn't already exist? Or at least warn when overwriting?
        String zipName = id + ".zip";
        String dbName = id + ".v2.db";
        String dbpName = id + ".v2.db.p";
        File zipFile = new File(cacheDir, zipName);
        File dbFile = new File(cacheDir, dbName);
        File dbpFile = new File(cacheDir, dbpName);

        // First try to load an already-existing GTFS MapDB cached locally.
        GTFSFeed feed;
        if (dbFile.exists()) {
            LOG.info("Processed GTFS was found cached locally.");
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

        // GTFS MapDB did not exist locally or was corrupted. Try to fetch an already-built GTFS MapDB from S3.
        if (bucket != null) {
            try {
                LOG.info("Attempting to download cached GTFS MapDB.");
                fetchFromS3(dbName, dbFile);
                fetchFromS3(dbpName, dbpFile);
                LOG.info("Returning processed GTFS from S3");
                feed = new GTFSFeed(dbFile.getAbsolutePath());
                if (feed != null) {
                    return feed;
                }
            } catch (AmazonS3Exception e) {
                LOG.warn("DB or DBP file {} does not exist on S3.", dbFile);
            } catch (ExecutionException | IOException e) {
                LOG.warn("Error retrieving MapDB from S3, will load from original GTFS.", e);
            }
        }

        // Fetching an already built MapDB from S3 was unsuccessful.
        // Instead, try to build a MapDB from the original GTFS ZIP in the local cache, and falling back to S3.
        if (zipFile.exists()) {
            LOG.info("Loading feed from local cache directory...");
        } else {
            if (bucket == null) {
                LOG.warn("No S3 bucket specified, nowhere to look for the origial GTFS ZIP file.");
            } else  {
                LOG.info("Feed not found locally, downloading from S3.");
                try {
                    fetchFromS3(zipName, zipFile);
                } catch (Exception e) {
                    // TODO fold this error reporting and exception handling into fetchFromS3 method.
                    LOG.error("Could not download feed at s3://{}/{}.", bucket, zipFile);
                    throw new RuntimeException(e);
                }
            }
        }

        if (zipFile.exists()) {
            // This will also re-upload the original feed ZIP to S3. TODO is this a problem or intentional behavior?
            try {
                return buildFeed(id, zipFile);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            LOG.warn("Feed {} not found locally", id);
            throw new NoSuchElementException(id);
        }
    }

    /**
     * Factored out the repeated action of fetching a file from S3 and storing in the local cache.
     * TODO fold error logging and exception handling into this method.
     */
    private void fetchFromS3 (String remoteName, File localFile) throws IOException, AmazonS3Exception {
        S3Object db = s3.getObject(bucket, remoteName);
        InputStream is = db.getObjectContent();
        FileOutputStream fos = new FileOutputStream(localFile);
        ByteStreams.copy(is, fos);
        is.close();
        fos.close();
    }

    /**
     * TODO explain why we would want to delete the local files.
     * Assumption is they are corrupted and should be rebuilt?
     */
    private void deleteLocalDBFiles(String id) {
        String dbName = id + ".v2.db";
        String dbpName = id + ".v2.db.p";
        File dbFile = new File(cacheDir, dbName);
        File dbpFile = new File(cacheDir, dbpName);
        dbFile.delete();
        dbpFile.delete();
    }

    /**
     * Replace all characters in the feed ID that are not alphanumeric or underscore with a dash.
     * This ensures that the ID is valid for use as a file name or object name on S3.
     * TODO maybe make a class for already cleaned IDs, with methods to get the DB, ZIP, JSON etc. filenames.
     */
    public static String cleanId(String id) {
        return id.replaceAll("[^A-Za-z0-9_]", "-");
    }

    /** Check that the supplied ID is "clean" for use as a filename and throw an exception if not. */
    public static void validateId (String id) {
        if (id == null || id.isBlank() || ! cleanId(id).equals(id)) {
            throw new RuntimeException("Supplied ID must consist entirely of alphanumeric characters or underscores.");
        }
    }

}
