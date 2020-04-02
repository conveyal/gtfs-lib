package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.error.GTFSError;
import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.model.*;
import com.conveyal.gtfs.storage.StorageException;
import com.conveyal.gtfs.util.InvalidNamespaceException;
import com.conveyal.gtfs.validator.*;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import static com.conveyal.gtfs.error.NewGTFSErrorType.VALIDATOR_FAILED;

/**
 * This connects to an SQL RDBMS containing GTFS data and lets you fetch elements out of it.
 */
public class Feed {

    private static final Logger LOG = LoggerFactory.getLogger(Feed.class);

    private final DataSource dataSource;

    // The unique database schema name for this particular feed, including the separator charater (dot).
    // This may be the empty string if the feed is stored in the root ("public") schema.
    public final String tablePrefix;

    public final TableReader<Agency> agencies;
    public final TableReader<Calendar> calendars;
    public final TableReader<CalendarDate> calendarDates;
    public final TableReader<FareAttribute> fareAttributes;
    public final TableReader<Frequency> frequencies;
    public final TableReader<Route> routes;
    public final TableReader<Stop>  stops;
    public final TableReader<Trip>  trips;
//    public final TableReader<ShapePoint> shapePoints;
    public final TableReader<StopTime>   stopTimes;

    /* A place to accumulate errors while the feed is loaded. Tolerate as many errors as possible and keep on loading. */
    // TODO remove this and use only NewGTFSErrors in Validators, loaded into a JDBC table
    public final List<GTFSError> errors = new ArrayList<>();

    /**
     * Create a feed that reads tables over a JDBC connection. The connection should already be set to the right
     * schema within the database.
     * @param tablePrefix the unique prefix for the table (may be null for no prefix)
     */
    public Feed (DataSource dataSource, String tablePrefix) {
        this.dataSource = dataSource;
        // Ensure separator dot is present
        if (tablePrefix != null && !tablePrefix.endsWith(".")) tablePrefix += ".";
        this.tablePrefix = tablePrefix == null ? "" : tablePrefix;
        agencies = new JDBCTableReader(Table.AGENCY, dataSource, tablePrefix, EntityPopulator.AGENCY);
        fareAttributes = new JDBCTableReader(Table.FARE_ATTRIBUTES, dataSource, tablePrefix, EntityPopulator.FARE_ATTRIBUTE);
        frequencies = new JDBCTableReader(Table.FREQUENCIES, dataSource, tablePrefix, EntityPopulator.FREQUENCY);
        calendars = new JDBCTableReader(Table.CALENDAR, dataSource, tablePrefix, EntityPopulator.CALENDAR);
        calendarDates = new JDBCTableReader(Table.CALENDAR_DATES, dataSource, tablePrefix, EntityPopulator.CALENDAR_DATE);
        routes = new JDBCTableReader(Table.ROUTES, dataSource, tablePrefix, EntityPopulator.ROUTE);
        stops = new JDBCTableReader(Table.STOPS, dataSource, tablePrefix, EntityPopulator.STOP);
        trips = new JDBCTableReader(Table.TRIPS, dataSource, tablePrefix, EntityPopulator.TRIP);
//        shapePoints = new JDBCTableReader(Table.SHAPES, dataSource, tablePrefix, EntityPopulator.SHAPE_POINT);
        stopTimes = new JDBCTableReader(Table.STOP_TIMES, dataSource, tablePrefix, EntityPopulator.STOP_TIME);
    }

    /**
     * TODO check whether validation has already occurred, overwrite results.
     * TODO allow validation within feed loading process, so the same connection can be used, and we're certain loaded data is 100% visible.
     * That would also avoid having to reconnect the error storage to the DB.
     */
    public ValidationResult validate (BiFunction<Feed, SQLErrorStorage, FeedValidator>... additionalValidators) {
        long validationStartTime = System.currentTimeMillis();
        // Create an empty validation result that will have its fields populated by certain validators.
        ValidationResult validationResult = new ValidationResult();
        // Error tables should already be present from the initial load.
        // Reconnect to the existing error tables.
        SQLErrorStorage errorStorage = null;
        try {
            errorStorage = new SQLErrorStorage(dataSource.getConnection(), tablePrefix, false);
        } catch (SQLException | InvalidNamespaceException ex) {
            throw new StorageException(ex);
        }
        int errorCountBeforeValidation = errorStorage.getErrorCount();

        List<FeedValidator> feedValidators = Lists.newArrayList(
            new MisplacedStopValidator(this, errorStorage, validationResult),
            new DuplicateStopsValidator(this, errorStorage),
            new FaresValidator(this, errorStorage),
            new FrequencyValidator(this, errorStorage),
            new TimeZoneValidator(this, errorStorage),
            new NewTripTimesValidator(this, errorStorage),
            new NamesValidator(this, errorStorage)
        );

        // Add additional validators, if any provided.
        for (BiFunction<Feed, SQLErrorStorage, FeedValidator> validator : additionalValidators) {
            FeedValidator v = validator.apply(this, errorStorage);
            feedValidators.add(v);
        }

        for (FeedValidator feedValidator : feedValidators) {
            String validatorName = feedValidator.getClass().getSimpleName();
            try {
                LOG.info("Running {}.", validatorName);
                int errorCountBefore = errorStorage.getErrorCount();
                // todo why not just pass the feed and errorstorage in here?
                feedValidator.validate();
                LOG.info("{} found {} errors.", validatorName, errorStorage.getErrorCount() - errorCountBefore);
            } catch (Exception e) {
                // store an error if the validator fails
                // FIXME: should the exception be stored?
                String badValue = String.join(":", validatorName, e.toString());
                errorStorage.storeError(NewGTFSError.forFeed(VALIDATOR_FAILED, badValue));
                LOG.error("{} failed.", validatorName);
                LOG.error(e.toString());
                e.printStackTrace();
            }
        }
        // Signal to all validators that validation is complete and allow them to report on results / status.
        for (FeedValidator feedValidator : feedValidators) {
            try {
                feedValidator.complete(validationResult);
            } catch (Exception e) {
                String badValue = String.join(":", feedValidator.getClass().getSimpleName(), e.toString());
                errorStorage.storeError(NewGTFSError.forFeed(VALIDATOR_FAILED, badValue));
                LOG.error("Validator failed completion stage.", e);
            }
        }
        // Total validation errors accounts for errors found during both loading and validation. Otherwise, this value
        // may be confusing if it reads zero but there were a number of data type or referential integrity errors found
        // during feed loading stage.
        int totalValidationErrors = errorStorage.getErrorCount();
        LOG.info("Errors found during load stage: {}", errorCountBeforeValidation);
        LOG.info("Errors found by validators: {}", totalValidationErrors - errorCountBeforeValidation);
        errorStorage.commitAndClose();
        long validationEndTime = System.currentTimeMillis();
        long totalValidationTime = validationEndTime - validationStartTime;
        LOG.info("{} validators completed in {} milliseconds.", feedValidators.size(), totalValidationTime);

        // update validation result fields
        validationResult.errorCount = totalValidationErrors;
        validationResult.validationTime = totalValidationTime;

        // FIXME: Validation result date and int[] fields need to be set somewhere.
        return validationResult;
    }

    /**
     * @return a JDBC connection to the database underlying this Feed.
     */
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

}
