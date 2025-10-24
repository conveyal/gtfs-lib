package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.error.NoAgencyInFeedError;

import java.io.IOException;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

public class Route extends Entity { // implements Entity.Factory<Route>

    private static final long serialVersionUID = -819444896818029068L;

    public static final int TRAM = 0;
    public static final int SUBWAY = 1;
    public static final int RAIL = 2;
    public static final int BUS = 3;
    public static final int FERRY = 4;
    public static final int CABLE_CAR = 5;
    public static final int GONDOLA = 6;
    public static final int FUNICULAR = 7;
    public static final int TROLLEY_BUS = 11;
    public static final int MONORAIL = 12;

    public String route_id;
    public String agency_id;
    public String route_short_name;
    public String route_long_name;
    public String route_desc;
    public int    route_type;
    public URL    route_url;
    public String route_color;
    public int route_sort_order;
    public String route_text_color;
    public URL route_branding_url;
    public String feed_id;
    public int continuous_pickup = INT_MISSING;
    public int continuous_drop_off = INT_MISSING;

    @Override
    public String getId () {
        return route_id;
    }

    /**
     * Maps this route's type to its corresponding standard route type (0-7).
     * If already a standard type (0-7), returns it unchanged.
     * Extended types (100+) are mapped to their standard equivalents.
     *
     * @return The standard route type (0-7) corresponding to this route's type
     */
    public int getStandardRouteType() {
        return RouteTypeUtil.getStandardRouteType(route_type);
    }

    /**
     * Returns the standard RouteType enum for this route, or null if not a standard type.
     * Only returns a value if route_type is 0-7, 11, or 12.
     *
     * @return The RouteType enum, or null if this route uses an extended type
     */
    public RouteType getRouteTypeEnum() {
        try {
            return RouteType.fromCode(route_type);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Returns the ExtendedRouteType enum for this route, or null if not found.
     * Returns a value only if the route_type matches a defined ExtendedRouteType.
     *
     * @return The ExtendedRouteType enum, or null if not found or if using a standard type
     */
    public ExtendedRouteType getExtendedRouteTypeEnum() {
        return ExtendedRouteType.fromCode(route_type);
    }

    /**
     * Returns true if this route uses an extended route type (100+).
     *
     * @return true if route_type >= 100, false otherwise
     */
    public boolean hasExtendedRouteType() {
        return RouteTypeUtil.isExtendedRouteType(route_type);
    }

    /**
     * Returns a human-readable description of this route's type.
     * For extended types, returns the detailed description.
     * For standard types, returns the enum name.
     *
     * @return A string description of the route type
     */
    public String getRouteTypeDescription() {
        ExtendedRouteType extended = getExtendedRouteTypeEnum();
        if (extended != null) {
            return extended.getDescription();
        }

        RouteType standard = getRouteTypeEnum();
        if (standard != null) {
            return standard.name();
        }

        return "Unknown route type: " + route_type;
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#ROUTES}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, route_id);
        statement.setString(oneBasedIndex++, agency_id);
        statement.setString(oneBasedIndex++, route_short_name);
        statement.setString(oneBasedIndex++, route_long_name);
        statement.setString(oneBasedIndex++, route_desc);
        setIntParameter(statement, oneBasedIndex++, route_type);
        statement.setString(oneBasedIndex++, route_url != null ? route_url.toString() : null);
        statement.setString(oneBasedIndex++, route_branding_url != null ? route_branding_url.toString() : null);
        statement.setString(oneBasedIndex++, route_color);
        statement.setString(oneBasedIndex++, route_text_color);
        // Editor-specific fields publicly_visible, wheelchair_accessible, route_sort_order, and status.
        setIntParameter(statement, oneBasedIndex++, 0);
        setIntParameter(statement, oneBasedIndex++, 0);
        // route_sort_order
        setIntParameter(statement, oneBasedIndex++, route_sort_order);
        setIntParameter(statement, oneBasedIndex++, 0);
        setIntParameter(statement, oneBasedIndex++, continuous_pickup);
        setIntParameter(statement, oneBasedIndex++, continuous_drop_off);
    }

    public static class Loader extends Entity.Loader<Route> {

        public Loader(GTFSFeed feed) {
            super(feed, "routes");
        }

        @Override
        protected boolean isRequired() {
            return true;
        }

        @Override
        public void loadOneRow() throws IOException {
            Route r = new Route();
            r.id = row + 1; // offset line number by 1 to account for 0-based row index
            r.route_id = getStringField("route_id", true);
            Agency agency = getRefField("agency_id", false, feed.agency);

            if (agency == null) {
                // if there is only one agency, associate with it automatically
                if (feed.agency.size() == 1) {
                    r.agency_id = feed.agency.values().iterator().next().agency_id;
                } else if (feed.agency.isEmpty()) {
                    feed.errors.add(new NoAgencyInFeedError());
                }
            } else {
                r.agency_id = agency.agency_id;
            }

            r.route_short_name = getStringField("route_short_name", false); // one or the other required, needs a special validator
            r.route_long_name = getStringField("route_long_name", false);
            r.route_desc = getStringField("route_desc", false);
            r.route_type = getIntField("route_type", true, 0, 1799);
            r.route_sort_order = getIntField("route_sort_order", false, 0, Integer.MAX_VALUE);
            r.route_url = getUrlField("route_url", false);
            r.route_color = getStringField("route_color", false);
            r.route_text_color = getStringField("route_text_color", false);
            r.route_branding_url = getUrlField("route_branding_url", false);
            r.continuous_pickup = getIntField("continuous_pickup", false, 0, 3, INT_MISSING);
            r.continuous_drop_off = getIntField("continuous_drop_off", false, 0, 3, INT_MISSING);
            r.feed = feed;
            r.feed_id = feed.feedId;
            // Attempting to put a null key or value will cause an NPE in BTreeMap
            if (r.route_id != null) feed.routes.put(r.route_id, r);
        }

    }

    public static class Writer extends Entity.Writer<Route> {    	
        public Writer (GTFSFeed feed) {
            super(feed, "routes");
        }

        @Override
        public void writeHeaders() throws IOException {
            writeStringField("agency_id");
            writeStringField("route_id");
            writeStringField("route_short_name");
            writeStringField("route_long_name");
            writeStringField("route_desc");
            writeStringField("route_type");
            writeStringField("route_url");
            writeStringField("route_color");
            writeStringField("route_text_color");
            writeStringField("route_branding_url");
            writeStringField("route_sort_order");
            writeStringField("continuous_pickup");
            writeStringField("continuous_drop_off");
            endRecord();
        }

        @Override
        public void writeOneRow(Route r) throws IOException {
            writeStringField(r.agency_id);
            writeStringField(r.route_id);
            writeStringField(r.route_short_name);
            writeStringField(r.route_long_name);
            writeStringField(r.route_desc);
            writeIntField(r.route_type);
            writeUrlField(r.route_url);
            writeStringField(r.route_color);
            writeStringField(r.route_text_color);
            writeUrlField(r.route_branding_url);
            writeIntField(r.route_sort_order);
            writeIntField(r.continuous_pickup);
            writeIntField(r.continuous_drop_off);
            endRecord();
        }

        @Override
        public Iterator<Route> iterator() {
            return feed.routes.values().iterator();
        }   	
    }
}
