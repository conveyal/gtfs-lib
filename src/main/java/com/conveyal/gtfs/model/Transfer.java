package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

public class Transfer extends Entity {

    private static final long serialVersionUID = -4944512120812641063L;
    public String from_stop_id;
    public String to_stop_id;
    public int  transfer_type;
    public int  min_transfer_time;
    public String from_route_id;
    public String to_route_id;
    public String from_trip_id;
    public String to_trip_id;

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#TRANSFERS}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, from_stop_id);
        statement.setString(oneBasedIndex++, to_stop_id);
        statement.setString(oneBasedIndex++, from_trip_id);
        statement.setString(oneBasedIndex++, to_trip_id);
        statement.setString(oneBasedIndex++, from_route_id);
        statement.setString(oneBasedIndex++, to_route_id);
        setIntParameter(statement, oneBasedIndex++, transfer_type);
        setIntParameter(statement, oneBasedIndex, min_transfer_time);
    }

    @Override
    public String getId() {
        return createId(from_stop_id, to_stop_id, from_trip_id, to_trip_id, from_route_id, to_route_id);
    }

    public static class Loader extends Entity.Loader<Transfer> {

        public Loader(GTFSFeed feed) {
            super(feed, "transfers");
        }

        @Override
        protected boolean isRequired() {
            return false;
        }

        @Override
        public void loadOneRow() throws IOException {
            Transfer tr = new Transfer();
            tr.id = row + 1; // offset line number by 1 to account for 0-based row index
            tr.from_stop_id      = getStringField("from_stop_id", false);
            tr.to_stop_id        = getStringField("to_stop_id", false);
            tr.from_route_id     = getStringField("from_route_id", false);
            tr.to_route_id       = getStringField("to_route_id", false);
            tr.from_trip_id      = getStringField("from_trip_id", false);
            tr.to_trip_id        = getStringField("to_trip_id", false);
            tr.transfer_type     = getIntField("transfer_type", true, 0, 3);
            tr.min_transfer_time = getIntField("min_transfer_time", false, 0, Integer.MAX_VALUE);

            getRefField("from_stop_id", false, feed.stops);
            getRefField("to_stop_id", false, feed.stops);
            getRefField("from_route_id", false, feed.routes);
            getRefField("to_route_id", false, feed.routes);
            getRefField("from_trip_id", false, feed.trips);
            getRefField("to_trip_id", false, feed.trips);

            tr.feed = feed;
            feed.transfers.put(
                createId(tr.from_stop_id, tr.to_stop_id, tr.from_trip_id, tr.to_trip_id, tr.from_route_id, tr.to_route_id),
                tr
            );
        }

    }

    public static class Writer extends Entity.Writer<Transfer> {
        public Writer (GTFSFeed feed) {
            super(feed, "transfers");
        }

        @Override
        protected void writeHeaders() throws IOException {
            writer.writeRecord(new String[] {
                "from_stop_id",
                "to_stop_id",
                "from_trip_id",
                "to_trip_id",
                "from_route_id",
                "to_route_id",
                "transfer_type",
                "min_transfer_time"
            });
        }

        @Override
        protected void writeOneRow(Transfer t) throws IOException {
            writeStringField(t.from_stop_id);
            writeStringField(t.to_stop_id);
            writeStringField(t.from_trip_id);
            writeStringField(t.to_trip_id);
            writeStringField(t.from_route_id);
            writeStringField(t.to_route_id);
            writeIntField(t.transfer_type);
            writeIntField(t.min_transfer_time);
            endRecord();
        }

        @Override
        protected Iterator<Transfer> iterator() {
            return feed.transfers.values().iterator();
        }
    }

    /**
     * Transfer entries have no ID in GTFS so we define one based on the fields in the transfer entry.
     */
    private static String createId(
        String fromStopId,
        String toStopId,
        String fromTripId,
        String toTripId,
        String fromRouteId,
        String toRouteId
    ) {
        return String.format("%s_%s_%s_%s_%s_%s", fromStopId, toStopId, fromTripId, toTripId, fromRouteId, toRouteId);
    }

}
