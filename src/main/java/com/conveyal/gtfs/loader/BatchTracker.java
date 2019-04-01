package com.conveyal.gtfs.loader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Avoid Java's "effectively final" nonsense when using prepared statements in foreach loops.
 * Automatically push execute batches of prepared statements before the batch gets too big.
 * TODO there's probably something like this in an Apache Commons util library
 */
public class BatchTracker {
    private static final Logger LOG = LoggerFactory.getLogger(BatchTracker.class);

    private final String recordType;
    private PreparedStatement preparedStatement;
    private int currentBatchSize = 0;
    private int totalRecordsProcessed = 0;

    public BatchTracker(String recordType, PreparedStatement preparedStatement) {
        this.preparedStatement = preparedStatement;
        this.recordType = recordType;
    }

    public void addBatch() throws SQLException {
        preparedStatement.addBatch();
        currentBatchSize += 1;
        if (currentBatchSize > JdbcGtfsLoader.INSERT_BATCH_SIZE) {
            preparedStatement.executeBatch();
            totalRecordsProcessed += currentBatchSize;
            currentBatchSize = 0;
        }
    }

    /**
     * Execute any remaining statements and return the total records processed.
     */
    public int executeRemaining() throws SQLException {
        if (currentBatchSize > 0) {
            totalRecordsProcessed += currentBatchSize;
            preparedStatement.executeBatch();
            currentBatchSize = 0;
        }
        // Avoid reuse, signal that this was cleanly closed.
        preparedStatement = null;
        LOG.info(String.format("Processed %d %s records", totalRecordsProcessed, recordType));
        return totalRecordsProcessed;
    }

    public void finalize () {
        if (preparedStatement != null || currentBatchSize > 0) {
            throw new RuntimeException("BUG: It looks like someone did not call executeRemaining on a BatchTracker.");
        }
    }
}
