package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;

import java.io.IOException;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Objects;

public class BookingRule extends Entity {

    private static final long serialVersionUID = -3961639608144161095L;

    public String booking_rule_id;
    public int booking_type = INT_MISSING;
    public int prior_notice_duration_min;
    public int prior_notice_duration_max;
    public int prior_notice_last_day;
    public int prior_notice_last_time = INT_MISSING;
    public int prior_notice_start_day;
    public int prior_notice_start_time = INT_MISSING;
    public String prior_notice_service_id;
    public String message;
    public String pickup_message;
    public String drop_off_message;
    public String phone_number;
    public URL info_url;
    public URL booking_url;

    @Override
    public String getId() {
        return booking_rule_id;
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#BOOKING_RULES}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, booking_rule_id);
        setIntParameter(statement, oneBasedIndex++, booking_type);
        setIntParameter(statement, oneBasedIndex++, prior_notice_duration_min);
        setIntParameter(statement, oneBasedIndex++, prior_notice_duration_max);
        setIntParameter(statement, oneBasedIndex++, prior_notice_last_day);
        setIntParameter(statement, oneBasedIndex++, prior_notice_last_time);
        setIntParameter(statement, oneBasedIndex++, prior_notice_start_day);
        setIntParameter(statement, oneBasedIndex++, prior_notice_start_time);
        statement.setString(oneBasedIndex++, prior_notice_service_id);
        statement.setString(oneBasedIndex++, message);
        statement.setString(oneBasedIndex++, pickup_message);
        statement.setString(oneBasedIndex++, drop_off_message);
        statement.setString(oneBasedIndex++, phone_number);
        statement.setString(oneBasedIndex++, info_url != null ? info_url.toString() : null);
        statement.setString(oneBasedIndex, booking_url != null ? booking_url.toString() : null);
    }

    public static class Loader extends Entity.Loader<BookingRule> {

        public Loader(GTFSFeed feed) {
            super(feed, "booking_rules");
        }

        @Override
        protected boolean isRequired() {
            return false;
        }

        @Override
        public void loadOneRow() throws IOException {
            BookingRule bookingRule = new BookingRule();
            bookingRule.id = row + 1; // offset line number by 1 to account for 0-based row index
            bookingRule.booking_rule_id = getStringField("booking_rule_id", true);
            bookingRule.booking_type = getIntField("booking_type", true, 0, 2);
            bookingRule.prior_notice_duration_min = getIntField("prior_notice_duration_min", false, 0, Integer.MAX_VALUE);
            bookingRule.prior_notice_duration_max = getIntField("prior_notice_duration_max", false, 0, Integer.MAX_VALUE);
            bookingRule.prior_notice_last_day = getIntField("prior_notice_last_day", false, 0, Integer.MAX_VALUE);
            bookingRule.prior_notice_last_time = getTimeField("prior_notice_last_time", false);
            bookingRule.prior_notice_start_day = getIntField("prior_notice_start_day", false, 0, Integer.MAX_VALUE);
            bookingRule.prior_notice_start_time = getTimeField("prior_notice_start_time", false);
            bookingRule.prior_notice_service_id = getStringField("prior_notice_service_id", false);
            bookingRule.message = getStringField("message", false);
            bookingRule.pickup_message = getStringField("pickup_message", false);
            bookingRule.drop_off_message = getStringField("drop_off_message", false);
            bookingRule.phone_number = getStringField("phone_number", false);
            bookingRule.info_url = getUrlField("info_url", false);
            bookingRule.booking_url = getUrlField("booking_url", false);

            // Attempting to put a null key or value will cause an NPE in BTreeMap
            if (bookingRule.booking_rule_id != null) {
                feed.bookingRules.put(bookingRule.booking_rule_id, bookingRule);
            }

            /*
              Check referential integrity without storing references. BookingRule cannot directly reference Calenders
              because they would be serialized into the MapDB.
             */
            getRefField("prior_notice_service_id", true, feed.calenders);
        }
    }

    public static class Writer extends Entity.Writer<BookingRule> {
        public Writer(GTFSFeed feed) {
            super(feed, "booking_rules");
        }

        @Override
        public void writeHeaders() throws IOException {
            writer.writeRecord(new String[]{"booking_rule_id", "booking_type", "prior_notice_duration_min",
                "prior_notice_duration_max", "prior_notice_last_day", "prior_notice_last_time", "prior_notice_start_day",
                "prior_notice_start_time", "prior_notice_service_id", "message", "pickup_message", "drop_off_message",
                "phone_number", "info_url", "booking_url"});
        }

        @Override
        public void writeOneRow(BookingRule bookingRule) throws IOException {
            writeStringField(bookingRule.booking_rule_id);
            writeIntField(bookingRule.booking_type);
            writeIntField(bookingRule.prior_notice_duration_min);
            writeIntField(bookingRule.prior_notice_duration_max);
            writeIntField(bookingRule.prior_notice_last_day);
            writeIntField(bookingRule.prior_notice_last_time);
            writeIntField(bookingRule.prior_notice_start_day);
            writeIntField(bookingRule.prior_notice_start_time);
            writeStringField(bookingRule.message);
            writeStringField(bookingRule.pickup_message);
            writeStringField(bookingRule.drop_off_message);
            writeStringField(bookingRule.phone_number);
            writeUrlField(bookingRule.info_url);
            writeUrlField(bookingRule.booking_url);
            endRecord();
        }

        @Override
        public Iterator<BookingRule> iterator() {
            return this.feed.bookingRules.values().iterator();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BookingRule that = (BookingRule) o;
        return booking_type == that.booking_type &&
            prior_notice_duration_min == that.prior_notice_duration_min &&
            prior_notice_duration_max == that.prior_notice_duration_max &&
            prior_notice_last_day == that.prior_notice_last_day &&
            prior_notice_last_time == that.prior_notice_last_time &&
            prior_notice_start_day == that.prior_notice_start_day &&
            prior_notice_start_time == that.prior_notice_start_time &&
            Objects.equals(booking_rule_id, that.booking_rule_id) &&
            Objects.equals(prior_notice_service_id, that.prior_notice_service_id) &&
            Objects.equals(message, that.message) &&
            Objects.equals(pickup_message, that.pickup_message) &&
            Objects.equals(drop_off_message, that.drop_off_message) &&
            Objects.equals(phone_number, that.phone_number) &&
            Objects.equals(info_url, that.info_url) &&
            Objects.equals(booking_url, that.booking_url);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            booking_rule_id,
            booking_type,
            prior_notice_duration_min,
            prior_notice_duration_max,
            prior_notice_last_day,
            prior_notice_last_time,
            prior_notice_start_day,
            prior_notice_start_time,
            prior_notice_service_id,
            message,
            pickup_message,
            drop_off_message,
            phone_number,
            info_url,
            booking_url
        );
    }
}
