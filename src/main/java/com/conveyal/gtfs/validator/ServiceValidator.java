package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.DateField;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.loader.JdbcGtfsLoader;
import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.model.Calendar;
import com.conveyal.gtfs.model.CalendarDate;
import com.conveyal.gtfs.model.Entity;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import com.conveyal.gtfs.storage.StorageException;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This will validate that service date information is coherent, and attempt to deduce or validate the range of dates
 * covered by a GTFS feed.
 *
 * It turns the GTFS system of repeating weekly calendars and exceptions (calendar dates) into a single large table
 * listing which services run on which days. This in turn allows us to build a histogram of service duration on each
 * day.
 *
 * As an intermediate result it builds a table of service duration by service ID and mode of transport.
 *
 * Makes one object representing each service ID.
 * That object will contain a calendar (for repeating service on specific days of the week)
 * and potentially multiple CalendarDates defining exceptions to the base calendar.
 * TODO build histogram of stop times, check against calendar and declared feed validity dates
 */
public class ServiceValidator extends TripValidator {

    private static final Logger LOG = LoggerFactory.getLogger(PatternFinderValidator.class);

    private Map<String, ServiceInfo> serviceInfoForServiceId = new HashMap<>();

    private Map<LocalDate, DateInfo> dateInfoForDate = new HashMap<>();

    public ServiceValidator(Feed feed, SQLErrorStorage errorStorage) {
        super(feed, errorStorage);
    }

    @Override
    public void validateTrip(Trip trip, Route route, List<StopTime> stopTimes, List<Stop> stops) {
        int firstStopDeparture = stopTimes.get(0).departure_time;
        int lastStopArrival = stopTimes.get(stopTimes.size() - 1).arrival_time;
        if (firstStopDeparture == Entity.INT_MISSING || lastStopArrival == Entity.INT_MISSING) {
            // ERR
            return;
        }
        int tripDurationSeconds = lastStopArrival - firstStopDeparture;
        if (tripDurationSeconds <= 0) {
            // ERR
            return;
        }
        // Get the map from modes to service durations in seconds for this trip's service ID.
        // Create a new empty map if it doesn't yet exist.
        ServiceInfo serviceInfo = serviceInfoForServiceId.computeIfAbsent(trip.service_id, ServiceInfo::new);
        // Increment the service duration for this trip's transport mode and service ID.
        serviceInfo.durationByRouteType.adjustOrPutValue(route.route_type, tripDurationSeconds, tripDurationSeconds);
        // Record which trips occur on each service_id.
        serviceInfo.tripIds.add(trip.trip_id);
        // TODO validate mode codes
    }

    /**
     * You'd think we'd want to do this during the loading phase. But during the loading phase we don't have a reading
     * connection to the entity tables in the database. Rather than make the Feed object read-write, we want to leave
     * it completely read-only.
     *
     * @param validationResult can be written into
     */
    @Override
    public void complete(ValidationResult validationResult) {

        LOG.info("Merging calendars and calendar_dates...");

        // First handle the calendar entries, which define repeating weekly schedules.
        for (Calendar calendar : feed.calendars) {
            try {
                LocalDate endDate = calendar.end_date;
                // Loop over all days in this calendar entry, recording on which ones it is active.
                for (LocalDate date = calendar.start_date; date.isBefore(endDate) || date.isEqual(endDate); date = date.plusDays(1)) {
                    DayOfWeek dayOfWeek = date.getDayOfWeek();
                    if (    (dayOfWeek == DayOfWeek.MONDAY && calendar.monday > 0) ||
                            (dayOfWeek == DayOfWeek.TUESDAY && calendar.tuesday > 0) ||
                            (dayOfWeek == DayOfWeek.WEDNESDAY && calendar.wednesday > 0) ||
                            (dayOfWeek == DayOfWeek.THURSDAY && calendar.thursday > 0) ||
                            (dayOfWeek == DayOfWeek.FRIDAY && calendar.friday > 0) ||
                            (dayOfWeek == DayOfWeek.SATURDAY && calendar.saturday > 0) ||
                            (dayOfWeek == DayOfWeek.SUNDAY && calendar.sunday > 0)) {
                        // Service is active on this date.
                        serviceInfoForServiceId.computeIfAbsent(calendar.service_id, ServiceInfo::new).datesActive.add(date);
                    }
                }
            } catch (Exception ex) {
                LOG.error(ex.getMessage());
                ex.printStackTrace();
                // Continue on to next calendar entry.
            }
        }

        // Next handle the calendar_dates, which specify exceptions to the repeating weekly schedules.
        for (CalendarDate calendarDate : feed.calendarDates) {
            ServiceInfo serviceInfo = serviceInfoForServiceId.computeIfAbsent(calendarDate.service_id, ServiceInfo::new);
            if (calendarDate.exception_type == 1) {
                // Service added, add to set for this date.
                serviceInfo.datesActive.add(calendarDate.date);
            } else if (calendarDate.exception_type == 2) {
                // Service removed, remove from Set for this date.
                serviceInfo.datesActive.remove(calendarDate.date);
            }
            // Otherwise exception_type is out of range. This should already have been caught during the loading phase.
        }

        /*
            A view that is similar to ServiceInfo class, but doesn't deal well with missing IDs in either subquery:
            select durations.service_id, duration_seconds, days_active from (
              (select service_id, sum(duration_seconds) as duration_seconds
                   from elwp_qhqsgzufnpvwnxtdbwcthn.service_durations group by service_id) as durations
              join
              (select service_id, count(service_date) as days_active
                   from elwp_qhqsgzufnpvwnxtdbwcthn.service_dates group by service_id) as days
              on durations.service_id = days.service_id
            );
         */


        // Check for incoherent or erroneous services.
        for (ServiceInfo serviceInfo : serviceInfoForServiceId.values()) {
            if (serviceInfo.datesActive.isEmpty()) {
                // This service must have been referenced by trips but is never active on any day.
                registerError(NewGTFSError.forFeed(NewGTFSErrorType.SERVICE_NEVER_ACTIVE, serviceInfo.serviceId));
                for (String tripId : serviceInfo.tripIds) {
                    registerError(NewGTFSError.forTable(Table.TRIPS, NewGTFSErrorType.TRIP_NEVER_ACTIVE).setBadValue(tripId));
                }
            }
            if (serviceInfo.tripIds.isEmpty()) {
                registerError(NewGTFSError.forFeed(NewGTFSErrorType.SERVICE_UNUSED, serviceInfo.serviceId));
            }
        }

        // Accumulate info about services into each date that they are active.
        for (ServiceInfo serviceInfo : serviceInfoForServiceId.values()) {
            for (LocalDate date : serviceInfo.datesActive) {
                dateInfoForDate.computeIfAbsent(date, DateInfo::new).add(serviceInfo);
            }
        }

        // Check for dates that have no service within full range of dates with defined service.
        // Sum up service duration by mode for each day within that range.
        if (dateInfoForDate.isEmpty()) {
            registerError(NewGTFSError.forFeed(NewGTFSErrorType.NO_SERVICE, null));
        } else {
            LocalDate firstDate = LocalDate.MAX;
            LocalDate lastDate = LocalDate.MIN;
            for (LocalDate date : dateInfoForDate.keySet()) {
                if (date.isBefore(firstDate)) firstDate = date;
                if (date.isAfter(lastDate)) lastDate = date;
            }
            TIntList busSeconds = new TIntArrayList();
            TIntList tramSeconds = new TIntArrayList();
            TIntList metroSeconds = new TIntArrayList();
            TIntList railSeconds = new TIntArrayList();
            TIntList totalSeconds = new TIntArrayList();
            for (LocalDate date = firstDate; date.isBefore(lastDate) || date.isEqual(lastDate); date = date.plusDays(1)) {
                // Add one value per day. Trove map returns zero for missing keys.
                DateInfo dateInfo = dateInfoForDate.get(date);
                if (dateInfo == null) {
                    registerError(NewGTFSError.forFeed(NewGTFSErrorType.DATE_NO_SERVICE, DateField.GTFS_DATE_FORMATTER.format(date)));
                    dateInfo = new DateInfo(date); // new empty object to get empty durations map.
                }
                busSeconds.add(dateInfo.durationByRouteType.get(3));
                tramSeconds.add(dateInfo.durationByRouteType.get(0));
                metroSeconds.add(dateInfo.durationByRouteType.get(1));
                railSeconds.add(dateInfo.durationByRouteType.get(2));
                totalSeconds.add(dateInfo.getTotalServiceDurationSeconds());
                // TODO check for low or zero service, which seems to happen even when services are defined.
            }
            validationResult.firstCalendarDate = firstDate;
            validationResult.lastCalendarDate = lastDate;
            validationResult.dailyBusSeconds = busSeconds.toArray();
            validationResult.dailyTramSeconds = tramSeconds.toArray();
            validationResult.dailyMetroSeconds = metroSeconds.toArray();
            validationResult.dailyRailSeconds = railSeconds.toArray();
            validationResult.dailyTotalSeconds = totalSeconds.toArray();
        }

        // Now write all these calendar-date relations out to the database.
        Connection connection = null;
        try {
            connection = feed.getConnection();
            Statement statement = connection.createStatement();

            // Create a table summarizing all known service IDs
            String servicesTableName = feed.tablePrefix + "services";
            String sql = String.format("create table %s (service_id varchar, days_active integer, duration_seconds integer)", servicesTableName);
            statement.execute(sql);
            sql = String.format("insert into %s values (?, ?, ?)", servicesTableName);
            PreparedStatement serviceStatement = connection.prepareStatement(sql);
            final BatchTracker serviceTracker = new BatchTracker(serviceStatement);
            for (ServiceInfo serviceInfo : serviceInfoForServiceId.values()) {
                serviceStatement.setString(1, serviceInfo.serviceId);
                serviceStatement.setInt(2, serviceInfo.datesActive.size());
                serviceStatement.setInt(3, serviceInfo.getTotalServiceDurationSeconds());
                serviceTracker.addBatch();
            }
            serviceTracker.executeRemaining();


            // Create a table that shows on which dates each service is active.
            String serviceDatesTableName = feed.tablePrefix + "service_dates";
            sql = String.format("create table %s (service_date varchar, service_id varchar)", serviceDatesTableName);
            statement.execute(sql);
            sql = String.format("insert into %s values (?, ?)", serviceDatesTableName);
            PreparedStatement serviceDateStatement = connection.prepareStatement(sql);
            final BatchTracker serviceDateTracker = new BatchTracker(serviceDateStatement);
            for (ServiceInfo serviceInfo : serviceInfoForServiceId.values()) {
                for (LocalDate date : serviceInfo.datesActive) {
                    if (date == null) continue; // TODO ERR? Can happen with bad data (unparseable dates).
                    try {
                        serviceDateStatement.setString(1, date.format(DateField.GTFS_DATE_FORMATTER));
                        serviceDateStatement.setString(2, serviceInfo.serviceId);
                        serviceDateTracker.addBatch();
                    } catch (SQLException e) {
                        throw new StorageException(e);
                    }
                }

            }
            serviceDateTracker.executeRemaining();
            LOG.info("Indexing...");
            statement.execute(String.format("create index service_dates_service_date on %s (service_date)", serviceDatesTableName));
            statement.execute(String.format("create index service_dates_service_id on %s (service_id)", serviceDatesTableName));

            // Save total trip durations per service_id and per transit mode into the database.
            /*
               -- Get total service duration by mode (route_type) per day, joining tables in database
               select service_date, route_type, sum(duration_seconds)
               from urhv_ghsubqdkatryosfaaeebdp.service_dates as dates, urhv_ghsubqdkatryosfaaeebdp.service_durations as durations
               where dates.service_id = durations.service_id
               group by service_date, route_type order by service_date, route_type;
            */
            String serviceDurationsTableName = feed.tablePrefix + "service_durations";
            sql = String.format("create table %s (service_id varchar, route_type integer, " +
                    "duration_seconds integer, primary key (service_id, route_type))", serviceDurationsTableName);
            statement.execute(sql);
            sql = String.format("insert into %s values (?, ?, ?)", serviceDurationsTableName);
            PreparedStatement serviceDurationStatement = connection.prepareStatement(sql);
            final BatchTracker serviceDurationTracker = new BatchTracker(serviceDurationStatement);
            for (ServiceInfo serviceInfo : serviceInfoForServiceId.values()) {
                serviceInfo.durationByRouteType.forEachEntry((routeType, serviceDurationSeconds) -> {
                    try {
                        serviceDurationStatement.setString(1, serviceInfo.serviceId);
                        serviceDurationStatement.setInt(2, routeType);
                        serviceDurationStatement.setInt(3, serviceDurationSeconds);
                        serviceDurationTracker.addBatch();
                    } catch (SQLException e) {
                        throw new StorageException(e);
                    }
                    return true; // Iteration continues
                });
            }
            serviceDurationTracker.executeRemaining();
            // No need to build indexes because (service_id, route_type) is already the primary key of this table.
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DbUtils.closeQuietly(connection);
        }
        LOG.info("Done.");
    }

    private static class ServiceInfo {

        final String serviceId;
        TIntIntHashMap durationByRouteType = new TIntIntHashMap();
        Set<LocalDate> datesActive = new HashSet<>();
        Set<String> tripIds = new HashSet<>();

        public ServiceInfo(String serviceId) {
            this.serviceId = serviceId;
        }

        public int getTotalServiceDurationSeconds() {
            return Arrays.stream(durationByRouteType.values()).sum();
        }

    }

    private static class DateInfo {

        final LocalDate date;
        TIntIntHashMap durationByRouteType = new TIntIntHashMap();
        Set<String> servicesActive = new HashSet<>();

        public DateInfo(LocalDate date) {
            this.date = date;
        }

        public int getTotalServiceDurationSeconds() {
            return Arrays.stream(durationByRouteType.values()).sum();
        }

        public void add (ServiceInfo serviceInfo) {
            servicesActive.add(serviceInfo.serviceId);
            serviceInfo.durationByRouteType.forEachEntry((routeType, serviceDurationSeconds) -> {
                durationByRouteType.adjustOrPutValue(routeType, serviceDurationSeconds, serviceDurationSeconds);
                return true; // Continue iteration.
            });
        }
    }

    /**
     * Avoid Java's "effectively final" nonsense when using prepared statements in foreach loops.
     * Automatically push execute batches of prepared statements before the batch gets too big.
     * TODO there's probably something like this in an Apache Commons util library
     */
    public static class BatchTracker {

        private PreparedStatement preparedStatement;
        private int currentBatchSize = 0;

        public BatchTracker(PreparedStatement preparedStatement) {
            this.preparedStatement = preparedStatement;
        }

        public void addBatch() throws SQLException {
            preparedStatement.addBatch();
            currentBatchSize += 1;
            if (currentBatchSize > JdbcGtfsLoader.INSERT_BATCH_SIZE) {
                preparedStatement.executeBatch();
                currentBatchSize = 0;
            }
        }

        public void executeRemaining() throws SQLException {
            if (currentBatchSize > 0) {
                preparedStatement.executeBatch();
                currentBatchSize = 0;
            }
            // Avoid reuse, signal that this was cleanly closed.
            preparedStatement = null;
        }

        public void finalize () {
            if (preparedStatement != null || currentBatchSize > 0) {
                throw new RuntimeException("BUG: It looks like someone did not call executeRemaining on a BatchTracker.");
            }
        }

    }

}
