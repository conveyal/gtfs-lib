package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.error.DuplicateKeyError;
import com.conveyal.gtfs.loader.DateField;
import com.conveyal.gtfs.loader.Table;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.Map;

public class Calendar extends Entity implements Serializable {

    private static final long serialVersionUID = 6634236680822635875L;

    public String service_id;

    public LocalDate start_date;
    public LocalDate end_date;

    // Should these be booleans? Oh... the implications.
    public int monday;
    public int tuesday;
    public int wednesday;
    public int thursday;
    public int friday;
    public int saturday;
    public int sunday;

    public String feed_id;

    @Override
    public String getId () {
        return service_id;
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#CALENDAR}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement) throws SQLException {
        DateField startDateField = (DateField) Table.CALENDAR.getFieldForName("start_date");
        DateField endDateField = ((DateField) Table.CALENDAR.getFieldForName("end_date"));
        statement.setInt(1, id);
        statement.setString(2, service_id);
        statement.setInt(3, monday);
        statement.setInt(4, tuesday);
        statement.setInt(5, wednesday);
        statement.setInt(6, thursday);
        statement.setInt(7, friday);
        statement.setInt(8, saturday);
        statement.setInt(9, sunday);
        startDateField.setParameter(statement, 10, start_date);
        endDateField.setParameter(statement, 11, end_date);
        statement.setString(12, null); // description
    }

    public static class Loader extends Entity.Loader<Calendar> {

        private final Map<String, Service> services;

        /**
         * Create a loader. The map parameter should be an in-memory map that will be modified. We can't write directly
         * to MapDB because we modify services as we load calendar dates, and this creates
         * ConcurrentModificationExceptions.
         */
        public Loader(GTFSFeed feed, Map<String, Service> services) {
            super(feed, "calendar");
            this.services = services;
        }

        @Override
        protected boolean isRequired() {
            return true;
        }

        @Override
        public void loadOneRow() throws IOException {

            /* Calendars and Fares are special: they are stored as joined tables rather than simple maps. */
            String service_id = getStringField("service_id", true); // TODO service_id can reference either calendar or calendar_dates.
            Service service = services.computeIfAbsent(service_id, Service::new);
            if (service.calendar != null) {
                feed.errors.add(new DuplicateKeyError(tableName, row, "service_id"));
            } else {
                Calendar c = new Calendar();
                c.id = row + 1; // offset line number by 1 to account for 0-based row index
                c.service_id = service.service_id;
                c.monday = getIntField("monday", true, 0, 1);
                c.tuesday = getIntField("tuesday", true, 0, 1);
                c.wednesday = getIntField("wednesday", true, 0, 1);
                c.thursday = getIntField("thursday", true, 0, 1);
                c.friday = getIntField("friday", true, 0, 1);
                c.saturday = getIntField("saturday", true, 0, 1);
                c.sunday = getIntField("sunday", true, 0, 1);
                // TODO check valid dates
                c.start_date = getDateField("start_date", true);
                c.end_date = getDateField("end_date", true);
                c.feed = feed;
                c.feed_id = feed.feedId;
                service.calendar = c;
            }

        }    
    }

    public static class Writer extends Entity.Writer<Calendar> {
        public Writer(GTFSFeed feed) {
            super(feed, "calendar");
        }

        @Override
        protected void writeHeaders() throws IOException {
            writer.writeRecord(new String[] {"service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "start_date", "end_date"});
        }

        @Override
        protected void writeOneRow(Calendar c) throws IOException {
            writeStringField(c.service_id);
            writeIntField(c.monday);
            writeIntField(c.tuesday);
            writeIntField(c.wednesday);
            writeIntField(c.thursday);
            writeIntField(c.friday);
            writeIntField(c.saturday);
            writeIntField(c.sunday);
            writeDateField(c.start_date);
            writeDateField(c.end_date);
            endRecord();
        }

        @Override
        protected Iterator<Calendar> iterator() {
            // wrap an iterator over services
            Iterator<Calendar> calIt = Iterators.transform(feed.services.values().iterator(), new Function<Service, Calendar> () {
                @Override
                public Calendar apply (Service s) {
                    return s.calendar;
                }
            });
            
            // not every service has a calendar (e.g. TriMet has no calendars, just calendar dates).
            // This is legal GTFS, so skip services with no calendar
            return Iterators.filter(calIt, new Predicate<Calendar> () {
                @Override
                public boolean apply(Calendar c) {
                    return c != null;
                }
            });
            
        }
    }
}
