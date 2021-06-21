package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;

import java.io.IOException;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

public class Attribution extends Entity {

    String attribution_id;
    String agency_id;
    String route_id;
    String trip_id;
    String organization_name;
    int is_producer;
    int is_operator;
    int is_authority;
    URL attribution_url;
    String attribution_email;
    String attribution_phone;

    @Override
    public String getId () {
        return attribution_id;
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#ATTRIBUTIONS}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, attribution_id);
        statement.setString(oneBasedIndex++, agency_id);
        statement.setString(oneBasedIndex++, route_id);
        statement.setString(oneBasedIndex++, trip_id);
        statement.setString(oneBasedIndex++, organization_name);
        setIntParameter(statement, oneBasedIndex++, is_producer);
        setIntParameter(statement, oneBasedIndex++, is_operator);
        setIntParameter(statement, oneBasedIndex++, is_authority);
        statement.setString(oneBasedIndex++, attribution_url != null ? attribution_url .toString() : null);
        statement.setString(oneBasedIndex++, attribution_email);
        statement.setString(oneBasedIndex++, attribution_phone);
    }

    public static class Loader extends Entity.Loader<Attribution> {

        public Loader(GTFSFeed feed) {
            super(feed, "attributions");
        }

        @Override
        protected boolean isRequired() {
            return false;
        }

        @Override
        public void loadOneRow() throws IOException {
            Attribution a = new Attribution();
            a.id = row + 1; // offset line number by 1 to account for 0-based row index
            a.attribution_id = getStringField("attribution_id", false);
            a.agency_id = getStringField("agency_id", false);
            a.route_id = getStringField("route_id", true);
            a.trip_id = getStringField("trip_id", false);
            a.organization_name = getStringField("organization_name", true);
            a.is_producer = getIntField("is_producer", false, 0, 1);
            a.is_operator = getIntField("is_operator", false, 0, 1);
            a.is_authority = getIntField("is_authority", false, 0, 1);
            a.attribution_url = getUrlField("attribution_url", false);
            a.attribution_email = getStringField("attribution_email", false);
            a.attribution_phone = getStringField("attribution_phone", false);

            // TODO clooge due to not being able to have null keys in mapdb
            if (a.attribution_id == null) a.attribution_id = "NONE";

            feed.attributions.put(a.attribution_id, a);
        }
    }

    public static class Writer extends Entity.Writer<Attribution> {
        public Writer (GTFSFeed feed) {
            super(feed, "attribution");
        }

        @Override
        protected void writeHeaders() throws IOException {
            writer.writeRecord(new String[] {"attribution_id", "agency_id", "route_id", "trip_id", "organization_name",
                "is_producer", "is_operator", "is_authority", "attribution_url", "attribution_email", "attribution_phone"});
        }

        @Override
        protected void writeOneRow(Attribution a) throws IOException {
            writeStringField(a.attribution_id);
            writeStringField(a.agency_id);
            writeStringField(a.route_id);
            writeStringField(a.trip_id);
            writeStringField(a.organization_name);
            writeIntField(a.is_producer);
            writeIntField(a.is_operator);
            writeIntField(a.is_authority);
            writeUrlField(a.attribution_url);
            writeStringField(a.attribution_email);
            writeStringField(a.attribution_phone);
            endRecord();
        }

        @Override
        protected Iterator<Attribution> iterator() {
            return feed.attributions.values().iterator();
        }
    }


}
