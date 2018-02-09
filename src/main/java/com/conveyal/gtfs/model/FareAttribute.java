package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.error.DuplicateKeyError;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;

public class FareAttribute extends Entity {

    private static final long serialVersionUID = 2157859372072056891L;
    public static final int UNLIMITED_TRANSFERS = Integer.MAX_VALUE;
    public String fare_id;
    public double price;
    public String currency_type;
    public int payment_method;
    public int transfers;
    public int transfer_duration;
    public String feed_id;

    @Override
    public String getId () {
        return fare_id;
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#FARE_ATTRIBUTES}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, fare_id);
        statement.setDouble(oneBasedIndex++, price);
        statement.setString(oneBasedIndex++, currency_type);
        setIntParameter(statement, oneBasedIndex++, payment_method);
        // FIXME Entity.INT_MISSING causing out of range error on small int
        setIntParameter(statement, oneBasedIndex++, transfers);
        setIntParameter(statement, oneBasedIndex++, transfer_duration);
    }

    public static class Loader extends Entity.Loader<FareAttribute> {
        private final Map<String, Fare> fares;

        public Loader(GTFSFeed feed, Map<String, Fare> fares) {
            super(feed, "fare_attributes");
            this.fares = fares;
        }

        @Override
        protected boolean isRequired() {
            return false;
        }

        @Override
        public void loadOneRow() throws IOException {

            /* Calendars and Fares are special: they are stored as joined tables rather than simple maps. */
            String fareId = getStringField("fare_id", true);
            Fare fare = fares.computeIfAbsent(fareId, Fare::new);
            if (fare.fare_attribute != null) {
                feed.errors.add(new DuplicateKeyError(tableName, row, "fare_id"));
            } else {
                FareAttribute fa = new FareAttribute();
                fa.id = row + 1; // offset line number by 1 to account for 0-based row index
                fa.fare_id = fareId;
                fa.price = getDoubleField("price", true, 0, Integer.MAX_VALUE);
                fa.currency_type = getStringField("currency_type", true);
                fa.payment_method = getIntField("payment_method", true, 0, 1);
                fa.transfers = getIntField("transfers", false, 0, 10, UNLIMITED_TRANSFERS); // in the GTFS spec, a missing value means "unlimited", so we default to UNLIMITED_TRANSFERS (or MAX_INT) when no value is found
                fa.transfer_duration = getIntField("transfer_duration", false, 0, 24 * 60 * 60);
                fa.feed = feed;
                fa.feed_id = feed.feedId;
                fare.fare_attribute = fa;
            }

        }

    }

    public static class Writer extends Entity.Writer<FareAttribute> {
        public Writer(GTFSFeed feed) {
            super(feed, "fare_attributes");
        }

        @Override
        public void writeHeaders() throws IOException {
            writer.writeRecord(new String[] {"fare_id", "price", "currency_type", "payment_method",
                    "transfers", "transfer_duration"});
        }

        @Override
        public void writeOneRow(FareAttribute fa) throws IOException {
            writeStringField(fa.fare_id);
            writeDoubleField(fa.price);
            writeStringField(fa.currency_type);
            writeIntField(fa.payment_method);
            writeIntField(fa.transfers);
            writeIntField(fa.transfer_duration);
            endRecord();
        }

        @Override
        public Iterator<FareAttribute> iterator() {
            return feed.fares.values().stream()
                    .map(f -> f.fare_attribute)
                    .iterator();
        }
    }

}
