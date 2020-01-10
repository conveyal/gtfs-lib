package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;

import java.io.IOException;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

public class Stop extends Entity {

    private static final long serialVersionUID = 464065335273514677L;
    public String stop_id;
    public String stop_code;
    public String stop_name;
    public String stop_desc;
    public double stop_lat;
    public double stop_lon;
    public String zone_id;
    public URL    stop_url;
    public int    location_type;
    public String parent_station;
    public String stop_timezone;
    // TODO should be int
    public String wheelchair_boarding;
    public String feed_id;

    @Override
    public String getId () {
        return stop_id;
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#STOPS}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        int wheelchairBoarding = 0;
        try {
             wheelchairBoarding = Integer.parseInt(wheelchair_boarding);
        } catch (NumberFormatException e) {
            // Do nothing, wheelchairBoarding will remain zero.
        }
        statement.setString(oneBasedIndex++, stop_id);
        statement.setString(oneBasedIndex++, stop_code);
        statement.setString(oneBasedIndex++, stop_name);
        statement.setString(oneBasedIndex++, stop_desc);
        statement.setDouble(oneBasedIndex++, stop_lat);
        statement.setDouble(oneBasedIndex++, stop_lon);
        statement.setString(oneBasedIndex++, zone_id);
        statement.setString(oneBasedIndex++, stop_url != null ? stop_url.toString() : null);
        setIntParameter(statement, oneBasedIndex++, location_type);
        statement.setString(oneBasedIndex++, parent_station);
        statement.setString(oneBasedIndex++, stop_timezone);
        // FIXME: For some reason wheelchair boarding type is String
        setIntParameter(statement, oneBasedIndex++, wheelchairBoarding);
    }

    public static class Loader extends Entity.Loader<Stop> {

        public Loader(GTFSFeed feed) {
            super(feed, "stops");
        }

        @Override
        protected boolean isRequired() {
            return true;
        }

        @Override
        public void loadOneRow() throws IOException {
            Stop s = new Stop();
            s.id = row + 1; // offset line number by 1 to account for 0-based row index
            s.stop_id   = getStringField("stop_id", true);
            s.stop_code = getStringField("stop_code", false);
            s.stop_name = getStringField("stop_name", true);
            s.stop_desc = getStringField("stop_desc", false);
            s.stop_lat  = getDoubleField("stop_lat", true, -90D, 90D);
            s.stop_lon  = getDoubleField("stop_lon", true, -180D, 180D);
            s.zone_id   = getStringField("zone_id", false);
            s.stop_url  = getUrlField("stop_url", false);
            s.location_type  = getIntField("location_type", false, 0, 1);
            s.parent_station = getStringField("parent_station", false);
            s.stop_timezone  = getStringField("stop_timezone", false);
            s.wheelchair_boarding = getStringField("wheelchair_boarding", false);
            s.feed = feed;
            s.feed_id = feed.feedId.get();
            /* TODO check ref integrity later, this table self-references via parent_station */
            // Attempting to put a null key or value will cause an NPE in BTreeMap
            if (s.stop_id != null) feed.stops.put(s.stop_id, s);
        }

    }

    public static class Writer extends Entity.Writer<Stop> {
        public Writer (GTFSFeed feed) {
            super(feed, "stops");
        }

        @Override
        public void writeHeaders() throws IOException {
            writer.writeRecord(new String[] {"stop_id", "stop_code", "stop_name", "stop_desc", "stop_lat", "stop_lon", "zone_id",					
                    "stop_url", "location_type", "parent_station", "stop_timezone", "wheelchair_boarding"});
        }

        @Override
        public void writeOneRow(Stop s) throws IOException {
            writeStringField(s.stop_id);
            writeStringField(s.stop_code);
            writeStringField(s.stop_name);
            writeStringField(s.stop_desc);
            writeDoubleField(s.stop_lat);
            writeDoubleField(s.stop_lon);
            writeStringField(s.zone_id);
            writeUrlField(s.stop_url);
            writeIntField(s.location_type);
            writeStringField(s.parent_station);
            writeStringField(s.stop_timezone);
            writeStringField(s.wheelchair_boarding);
            endRecord();
        }

        @Override
        public Iterator<Stop> iterator() {
            return feed.stops.values().iterator();
        }   	
    }
}
