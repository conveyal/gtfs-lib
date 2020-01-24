package com.conveyal.gtfs;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.S3Object;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
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
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.zip.ZipFile;

/**
 * Cache for GTFSFeed objects, a disk-backed (MapDB) representation of data from one GTFS feed. The source GTFS
 * feeds and backing MapDB files are themselves cached in the local filesystem and mirrored to S3 for reuse by later
 * deployments and worker machines. GTFSFeeds may be evicted from the in-memory cache, at which time they will be
 * closed. Any code continuing to hold a reference to the evicted GTFSFeed will then fail if it tries to access the
 * closed MapDB. The exact eviction policy is discussed in Javadoc on the class fields and methods.
 */
public class GTFSCache {

    private static final Logger LOG = LoggerFactory.getLogger(GTFSCache.class);

    private static final int MAX_CACHE_SIZE = 20;
    private static final int EXPIRE_AFTER_ACCESS_MINUTES = 60;

    private static final String GTFS_EXTENSION = ".zip";
    private static final String DB_EXTENSION = ".db";
    private static final String DBP_EXTENSION = ".db.p";

    /** The bucket on S3 to which the source GTFS feeds and MapDB files will be mirrored for long term storage. */
    public final String bucket;

    /** The local filesystem directory where the source GTFS feeds and files backing the MapDB tables are stored. */
    public final File cacheDir;

    /** S3 client, should eventually be merged with all other S3-backed caches. */
    private static AmazonS3 s3 = null;

    /** A Caffeine LoadingCache holding open GTFSFeed instances. */
    private final LoadingCache<String, GTFSFeed> cache;

    /** If bucket is null, work offline and do not use S3 */
    public GTFSCache (String awsRegion, String bucket, File cacheDir) {
        if (awsRegion == null || bucket == null) {
            LOG.info("No AWS region/bucket specified. GTFSCache will not store or retrieve files from S3.");
        } else {
            s3 = AmazonS3ClientBuilder.standard().withRegion(awsRegion).build();
            LOG.info("Using AWS S3 bucket {} for GTFSCache", bucket);
        }
        this.bucket = bucket;
        this.cacheDir = cacheDir;
        this.cache = makeCaffeineCache();
    }

    /**
     * Each GTFSFeed instance is an abstraction over a set of MapDB tables backed by disk files. We cannot allow more
     * than instance actively representing the same feed, as this would corrupt the underlying disk files.
     *
     * A caller can hold a reference to a GTFSFeed for an indefinite amount of time. If during that time the GTFSFeed is
     * evicted from the cache then requested again, a new GTFSFeed instance would be created by the CacheLoader and
     * connected to the same backing files. Therefore our eviction listener closes the MapDB when it is evicted.
     * This means that callers still holding references will no longer be able to use their reference, as it is closed.
     * Closing on eviction is the only way to safely avoid file corruption, and in practice poses no problems as this
     * system is only used for exposing GTFS contents over a GraphQL API. At worst an API call would fail and have to
     * be re-issued by the client.
     *
     * Another approach is to use SoftReferences for the cache values, which will not be evicted if they are referenced
     * elsewhere. This would eliminate the problem of callers holding references to closed GTFSFeeds, however eviction
     * is then based on heap memory demand, as perceived by the garbage collector. This is quite unpredictable - it
     * could decide to evict ten feeds that are unreferenced but last used only seconds ago, when memory could have
     * been freed by a GC pass. Caffeine cache documentation recommends using "the more predictable maximum cache
     * size" instead. Size-based eviction will "try to evict entries that have not been used recently or very often".
     * Maximum size eviction can be combined with time-based eviction.
     *
     * The memory cost of each open MapDB should just be the size-limited "instance cache", and any off-heap address
     * space allocated to memory-mapped files. I believe this cost should be roughly constant per feed. The
     * maximum size interacts with the instance cache size of MapDB itself, and whether MapDB memory is on or off heap.
     */
    private LoadingCache makeCaffeineCache() {
        RemovalListener<String, GTFSFeed> removalListener = (uniqueId, feed, cause) -> {
            LOG.info("Evicting feed {} from GTFSCache and closing MapDB file. Reason: {}", uniqueId, cause);
            // Close DB to avoid leaking (off-heap allocated) memory for MapDB object cache, and MapDB corruption.
            feed.close();
        };
        return Caffeine.newBuilder()
                .maximumSize(MAX_CACHE_SIZE)
                .expireAfterAccess(EXPIRE_AFTER_ACCESS_MINUTES, TimeUnit.MINUTES)
                .removalListener(removalListener)
                .build(this::retrieveAndProcessFeed);
    }

    /**
     * Build the MapDB files for the given GTFS feed ZIP, and return the GTFSFeed wrapping the open MapDB files,
     * WITHOUT putting the GTFSFeed in the cache.
     *
     * The supplied String id should not be the feed's self-declared feed ID, because we may load multiple versions
     * of the same feed.
     * TODO clarify behavior with respect to the supplied ID. Scope, interference with ID generator function, etc.
     * If an idGenerator function is supplied it will generate a unique identifier for the feed (in case multiple
     * versions of the same feed are loaded). The function is evaluated after the feed is already loaded, so it can
     * examine the contents of the feed when generating the ID.
     * TODO explain overwriting behavior if files already exist.
     */
    public GTFSFeed buildFeed (String id, File feedFile, Function<GTFSFeed, String> idGenerator) throws Exception {
        // generate temporary ID to name files
        String tempId = id != null ? id : UUID.randomUUID().toString();

        // read the feed
        String cleanTempId = cleanId(tempId);
        File dbFile = new File(cacheDir, cleanTempId + DB_EXTENSION);
        File movedFeedFile = new File(cacheDir, cleanTempId + GTFS_EXTENSION);

        // Copy the file into the local cache unless we're loading from a feed that is already in that local cache.
        // Should we move the file instead?
        if (!feedFile.equals(movedFeedFile)) {
            Files.copy(feedFile, movedFeedFile);
        }

        // Load the GTFS data into the GTFSFeed MapDB tables.
        GTFSFeed feed = new GTFSFeed(dbFile.getAbsolutePath());
        feed.loadFromFile(new ZipFile(movedFeedFile));
        feed.findPatterns();

        // Apply the ID generator function after the feed has been loaded, so it can look inside the feed.
        if (idGenerator != null) {
            id = idGenerator.apply(feed);
        }
        String cleanId = cleanId(id);

        // Close the DB and flush to disk before we start moving and copying files around.
        feed.close();

        // This appears to only be necessary because we are changing the name of the files after reading the feed,
        // so we can rename them based on the contents of the feed (the declared feed_id). We should probably just
        // supply unique IDs (e.g. from Mongo records) that don't contain the declared feed_id.
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

        // Upload original feed zip and MapDB files to S3 for long-term retrieval by workers and future deployments.
        if (bucket != null) {
            LOG.info("Uploading GTFS feed and MapDB files to S3 for long-term storage...");

            // Only upload the ZIP if it's not already on S3. We might be rebuilding MapDB files from an existing zip.
            if (!s3.doesObjectExist(bucket, cleanId + GTFS_EXTENSION)) {
                s3.putObject(bucket, cleanId + GTFS_EXTENSION, feedFile);
                LOG.info("GTFS ZIP file uploaded to S3.");
            } else {
                LOG.info("GTFS ZIP file already exists on S3, it was not uploaded.");
            }
            // Always upload the MapDB files since calling this method is a request for them to be (re)built.
            s3.putObject(bucket, cleanId + DB_EXTENSION, new File(cacheDir, cleanId + DB_EXTENSION));
            s3.putObject(bucket, cleanId + DBP_EXTENSION, new File(cacheDir, cleanId + DBP_EXTENSION));
            LOG.info("GTFS MapDB files uploaded to S3.");
        }

        // Reopen the feed database so we can return it ready for use to the caller. Note that we do not add the feed
        // to the cache here. The returned feed is inserted automatically into the LoadingCache by the CacheLoader.
        feed = new GTFSFeed(new File(cacheDir, cleanId + DB_EXTENSION).getAbsolutePath());
        return feed;
    }

    public GTFSFeed get (String uniqueId) {
        try {
            return cache.get(uniqueId);
        } catch (Exception e) {
            LOG.error("Error in GTFSCache get method, deleting local MapDB files which may be corrupted.", e);
            deleteLocalDBFiles(uniqueId);
            throw new RuntimeException("Error loading or building GTFSFeed.", e);
        }
    }

    /** retrieve a feed from local cache or S3 */
    private GTFSFeed retrieveAndProcessFeed (String originalId) {

        // Derive file names and File objects from the supplied ID.
        // TODO ideally "cleaning" would be an assertion instead of transforming the supplied ID.
        String id = cleanId(originalId);

        String dbName = id + DB_EXTENSION;
        String dbpName = id + DBP_EXTENSION;
        String zipName = id + GTFS_EXTENSION;

        File dbFile = new File(cacheDir, dbName);
        File dbpFile = new File(cacheDir, dbpName);
        File zipFile = new File(cacheDir, zipName);

        // First try to load an already-existing GTFS MapDB cached locally
        GTFSFeed feed;
        if (dbFile.exists()) {
            LOG.info("Processed GTFS MapDB file was found in local filesystem.");
            try {
                feed = new GTFSFeed(dbFile.getAbsolutePath());
                if (feed != null) {
                    return feed;
                }
            } catch (Exception e) {
                // Delete any MapDB files and fall through to the steps that will rebuild them,
                // which will hopefully have a different (better) outcome.
                LOG.error("Exception opening GTFSFeed, deleting (corrupted?) local MapDB files and rebuilding.", e);
                deleteLocalDBFiles(id);
            }
        }

        // Fallback: try to fetch an already-built GTFS MapDB from S3
        if (bucket != null) {
            try {
                LOG.info("Attempting to download cached GTFS MapDB from S3.");
                fetchFromS3(dbName, dbFile);
                fetchFromS3(dbpName, dbpFile);
                LOG.info("Retrieved GTFS MapDB from S3, connecting to it.");
                feed = new GTFSFeed(dbFile.getAbsolutePath());
                if (feed != null) {
                    return feed;
                }
            } catch (AmazonS3Exception e) {
                LOG.warn("GTFS MapDB file for with uniqueId {} does not exist on S3.", id);
            } catch (ExecutionException | IOException e) {
                LOG.warn("Error retrieving or opening MapDB from S3.", e);
            }
        }

        // Fetching an already built MapDB from S3 was unsuccessful.
        // Instead, try to build a MapDB from the original GTFS ZIP in the local cache, again falling back to S3.
        LOG.info("Building or rebuilding MapDB from original GTFS ZIP file...");
        if (zipFile.exists()) {
            LOG.info("Loading existing original GTFS ZIP feed from local filesystem...");
        }

        if (!zipFile.exists() && bucket != null) {
            LOG.info("Original GTFS ZIP not found on local filesystem, attempting download from S3.");
            try {
                fetchFromS3(zipName, zipFile);
            } catch (Exception e) {
                LOG.error("Could not download original GTFS ZIP file at s3://{}/{}.", bucket, id);
                throw new RuntimeException(e);
            }
        }

        // Rebuild from the original GTFS ZIP file if it was retrievable. Otherwise, we have to give up.
        if (zipFile.exists()) {
            // TODO this will also re-upload the original feed ZIP to S3, as if a user had just added it to Analysis.
            try {
                return buildFeed(originalId, zipFile, null);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            LOG.error("Original GTFS ZIP for {} could not be found anywhere (local or S3).", originalId);
            throw new NoSuchElementException(originalId);
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
     * Delete the local MapDB files. They can be corrupted by interrupted downloads or bugs that cause concurrent
     * changes. By deleting them we cause them to be rebuilt from the source GTFS ZIP file later.
     */
    private void deleteLocalDBFiles (String id) {
        (new File(cacheDir, id + DB_EXTENSION)).delete();
        (new File(cacheDir, id + DBP_EXTENSION)).delete();
    }

    public static String cleanId(String id) {
        // replace all special characters with `-`, except for underscore `_`
        return id.replaceAll("[^A-Za-z0-9_]", "-");
    }
}
