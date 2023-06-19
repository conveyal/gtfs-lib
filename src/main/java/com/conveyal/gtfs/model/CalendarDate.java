package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.error.DuplicateKeyError;
import com.conveyal.gtfs.loader.DateField;
import com.conveyal.gtfs.loader.Table;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;

public class CalendarDate extends Entity implements Cloneable, Serializable {

    private static final long serialVersionUID = 6936614582249119431L;
    public String    service_id;
    public LocalDate date;
    public int       exception_type;

    @Override
    public String getId () {
        return service_id;
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#CALENDAR_DATES}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        DateField dateField = (DateField) Table.CALENDAR_DATES.getFieldForName("date");
        statement.setString(oneBasedIndex++, service_id);
        dateField.setParameter(statement, oneBasedIndex++, date);
        setIntParameter(statement, oneBasedIndex++, exception_type);
    }

    public CalendarDate clone () {
        try {
            return (CalendarDate) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    public static class Loader extends Entity.Loader<CalendarDate> {

        private final Map<String, Service> services;

        /**
         * Create a loader. The map parameter should be an in-memory map that will be modified. We can't write directly
         * to MapDB because we modify services as we load calendar dates, and this creates concurrentmodificationexceptions.
         */
        public Loader(GTFSFeed feed, Map<String, Service> services) {
            super(feed, "calendar_dates");
            this.services = services;
        }

        @Override
        protected boolean isRequired() {
            return false;
        }

        @Override
        public void loadOneRow() throws IOException {
            /* Calendars and calendar dates that are matched on service id are special: they are stored as joined tables
             rather than simple maps. */
            String serviceId = getStringField("service_id", true);
            LocalDate date = getDateField("date", true);
            CalendarDate cd = new CalendarDate();
            cd.id = row + 1; // offset line number by 1 to account for 0-based row index
            cd.service_id = serviceId;
            cd.date = date;
            cd.exception_type = getIntField("exception_type", true, 1, 2);
            cd.feed = feed;
            if (services.containsKey(serviceId)) {
                // Calendar date is related to calendar.
                Service service = services.computeIfAbsent(serviceId, Service::new);
                if (service.calendar_dates.containsKey(date)) {
                    feed.errors.add(new DuplicateKeyError(tableName, row, "(service_id, date)"));
                } else {
                    service.calendar_dates.put(date, cd);
                }
            } else {
                // Calendar date is a stand-alone date that is not related to a calendar.
                if (cd.service_id != null) feed.calendarDates.put(cd.service_id, cd);
            }
        }
    }

    public static class Writer extends Entity.Writer<CalendarDate> {
        public Writer (GTFSFeed feed) {
            super(feed, "calendar_dates");
        }

        @Override
        protected void writeHeaders() throws IOException {
            writer.writeRecord(new String[] {"service_id", "date", "exception_type"});
        }

        @Override
        protected void writeOneRow(CalendarDate d) throws IOException {
            writeStringField(d.service_id);
            writeDateField(d.date);
            writeIntField(d.exception_type);
            endRecord();
        }

        @Override
        protected Iterator<CalendarDate> iterator() {
            // Combine all calendar dates references. This is only used to write to table/file.
            Iterator<Service> serviceIterator = feed.services.values().iterator();
            Iterator<CalendarDate> calendarDatesFromServices = Iterators.concat(
                Iterators.transform(serviceIterator, service -> service.calendar_dates.values().iterator())
            );
            Iterator<CalendarDate> calendarDatesIterator = feed.calendarDates.values().iterator();
            return Iterators.concat(
                calendarDatesIterator, calendarDatesFromServices
            );
        }
    }
}
