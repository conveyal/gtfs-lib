package com.conveyal.gtfs.loader;

/**
 * This contains the result of a feed snapshot operation. It is nearly identical to {@link FeedLoadResult} except that
 * it has some additional tables that only exist for snapshots/editor feeds.
 */
public class SnapshotResult extends FeedLoadResult {
    private static final long serialVersionUID = 1L;

    public TableLoadResult scheduleExceptions;
}
