package com.conveyal.gtfs;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
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
 * Fast cache for GTFS feeds stored on S3.
 */
public class GTFSCache {
    private static final Logger LOG = LoggerFactory.getLogger(GTFSCache.class);

    public final String bucket;
    public final String bucketFolder;

    public final File cacheDir;
    public long cacheSize;
    private static final long DEFAULT_CACHE_SIZE = 10L;

    private static final AmazonS3 s3 = new AmazonS3Client();
    private LoadingCache<String, GTFSFeed> cache;

    public GTFSCache(String bucket, File cacheDir) {
        this(bucket, null, cacheDir);
    }

    public GTFSCache(String bucket, String bucketFolder, File cacheDir) {
        this(bucket, bucketFolder, cacheDir, DEFAULT_CACHE_SIZE);
    }

    /** If bucket is null, work offline and do not use S3 */
    public GTFSCache(String bucket, String bucketFolder, File cacheDir, long cacheSize) {
        if (bucket == null) LOG.info("No bucket specified; GTFS Cache will run locally");
        else LOG.info("Using bucket {} for GTFS Cache", bucket);

        this.bucket = bucket;
        this.bucketFolder = bucketFolder != null ? bucketFolder.replaceAll("\\/","") : null;

        this.cacheDir = cacheDir;
        this.cacheSize = cacheSize;

        if (bucket != null) {
            LOG.warn("Local cache files (including .zip) will be deleted when removed from cache.");
        }
        RemovalListener<String, GTFSFeed> removalListener = removalNotification -> {
            // delete local files ONLY if using s3
            if (bucket != null) {
                String id = removalNotification.getKey();
                String[] extensions = {".db", ".db.p", ".zip"};
                // delete local cache files (including zip) when feed removed from cache
                for (String type : extensions) {
                    File file = new File(cacheDir, id + type);
                    file.delete();
                }
            }
        };
        this.cache = CacheBuilder.newBuilder()
                .maximumSize(cacheSize)
                .removalListener(removalListener)
                .build(new CacheLoader<String, GTFSFeed>() {
                    @Override
                    public GTFSFeed load(String s) throws Exception {
                        return retrieveFeed(s);
                    }
                });
    }

    public long getCurrentCacheSize() {
        return this.cache.size();
    }

    /**
     * Add a GTFS feed to this cache with the given ID. NB this is not the feed ID, because feed IDs are not
     * unique when you load multiple versions of the same feed.
     */
    public GTFSFeed put (String id, File feedFile) throws Exception {
        return put(id, feedFile, null);
    }

    /** Add a GTFS feed to this cache where the ID is calculated from the feed itself */
    public GTFSFeed put (Function<GTFSFeed, String> idGenerator, File feedFile) throws Exception {
        return put(null, feedFile, idGenerator);
    }

    private GTFSFeed put (String id, File feedFile, Function<GTFSFeed, String> idGenerator) throws Exception {
        // generate temporary ID to name files
        String tempId = id != null ? id : UUID.randomUUID().toString();

        // read the feed
        String cleanTempId = cleanId(tempId);
        File dbFile = new File(cacheDir, cleanTempId + ".db");
        File movedFeedFile = new File(cacheDir, cleanTempId + ".zip");

        // don't copy if we're loading from a locally-cached feed
        if (!feedFile.equals(movedFeedFile)) Files.copy(feedFile, movedFeedFile);

        GTFSFeed feed = new GTFSFeed(dbFile.getAbsolutePath());
        feed.loadFromFile(new ZipFile(movedFeedFile));
        feed.findPatterns();

        if (idGenerator != null) id = idGenerator.apply(feed);

        String cleanId = cleanId(id);

        feed.close(); // make sure everything is written to disk

        if (idGenerator != null) {
            new File(cacheDir, cleanTempId + ".zip").renameTo(new File(cacheDir, cleanId + ".zip"));
            new File(cacheDir, cleanTempId + ".db").renameTo(new File(cacheDir, cleanId + ".db"));
            new File(cacheDir, cleanTempId + ".db.p").renameTo(new File(cacheDir, cleanId + ".db.p"));
        }

        // upload feed
        // TODO best way to do this? Should we zip the files together?
        if (bucket != null) {
            LOG.info("Writing feed to s3 cache");
            String key = bucketFolder != null ? String.join("/", bucketFolder, cleanId) : cleanId;

            // write zip to s3 if not already there
            if (!s3.doesObjectExist(bucket, key + ".zip")) {
                s3.putObject(bucket, key + ".zip", feedFile);
                LOG.info("Zip file written.");
            }
            else {
                LOG.info("Zip file already exists on s3.");
            }
            s3.putObject(bucket, key + ".db", new File(cacheDir, cleanId + ".db"));
            s3.putObject(bucket, key + ".db.p", new File(cacheDir, cleanId + ".db.p"));
            LOG.info("db files written.");
        }

        // reconnect to feed database
        feed = new GTFSFeed(new File(cacheDir, cleanId + ".db").getAbsolutePath());
        cache.put(id, feed);
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

    public boolean containsId (String id) {
        GTFSFeed feed;
        try {
            feed = cache.get(id);
        } catch (Exception e) {
            return false;
        }
        return feed != null;
    }


    /** retrieve a feed from local cache or S3 */
    protected GTFSFeed retrieveFeed (String originalId) {
        // see if we have it cached locally
        String id = cleanId(originalId);
        String key = bucketFolder != null ? String.join("/", bucketFolder, id) : id;
        File dbFile = new File(cacheDir, id + ".db");
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

        if (bucket != null) {
            try {
                LOG.info("Attempting to download cached GTFS MapDB.");
                S3Object db = s3.getObject(bucket, key + ".db");
                InputStream is = db.getObjectContent();
                FileOutputStream fos = new FileOutputStream(dbFile);
                ByteStreams.copy(is, fos);
                is.close();
                fos.close();

                S3Object dbp = s3.getObject(bucket, key + ".db.p");
                InputStream isp = dbp.getObjectContent();
                FileOutputStream fosp = new FileOutputStream(new File(cacheDir, id + ".db.p"));
                ByteStreams.copy(isp, fosp);
                isp.close();
                fosp.close();

                LOG.info("Returning processed GTFS from S3");
                feed = new GTFSFeed(dbFile.getAbsolutePath());
                if (feed != null) {
                    return feed;
                }
            } catch (AmazonS3Exception e) {
                LOG.warn("DB file for key {} does not exist on S3.", key);
            } catch (ExecutionException | IOException e) {
                LOG.warn("Error retrieving MapDB from S3, will load from original GTFS.", e);
            }
        }

        // see if the

        // if we fell through to here, getting the mapdb was unsuccessful
        // grab GTFS from S3 if it is not found locally
        File feedFile = new File(cacheDir, id + ".zip");
        if (feedFile.exists()) {
            LOG.info("Loading feed from local cache directory...");
        }

        if (!feedFile.exists() && bucket != null) {
            LOG.info("Feed not found locally, downloading from S3.");
            try {
                S3Object gtfs = s3.getObject(bucket, key + ".zip");
                InputStream is = gtfs.getObjectContent();
                FileOutputStream fos = new FileOutputStream(feedFile);
                ByteStreams.copy(is, fos);
                is.close();
                fos.close();
            } catch (Exception e) {
                LOG.error("Could not download feed at s3://{}/{}.", bucket, key);
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
            LOG.warn("Feed {} found neither locally nor on S3", originalId);
            throw new NoSuchElementException(originalId);
        }
    }

    private void deleteLocalDBFiles(String id) {
        String[] extensions = {".db", ".db.p"};
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
