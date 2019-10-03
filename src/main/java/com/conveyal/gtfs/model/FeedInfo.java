package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.error.GeneralError;
import com.conveyal.gtfs.loader.DateField;
import com.conveyal.gtfs.loader.Table;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;

import java.io.IOException;
import java.net.URL;
import java.util.Iterator;

public class FeedInfo extends Entity implements Cloneable {

    private static final long serialVersionUID = 8718856987299076452L;
    public String    feed_id = "NONE";
    public String    feed_publisher_name;
    public URL       feed_publisher_url;
    public String    feed_lang;
    public LocalDate feed_start_date;
    public LocalDate feed_end_date;
    public String    feed_version;

    public FeedInfo clone () {
        try {
            return (FeedInfo) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#FEED_INFO}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        DateField feedStartDateField = (DateField) Table.FEED_INFO.getFieldForName("feed_start_date");
        DateField feedEndDateField = (DateField) Table.FEED_INFO.getFieldForName("feed_end_date");
        String feedPublisherUrl = feed_publisher_url != null ? feed_publisher_url.toString() : null;
        statement.setString(oneBasedIndex++, feed_id);
        statement.setString(oneBasedIndex++, feed_publisher_name);
        statement.setString(oneBasedIndex++, feedPublisherUrl);
        statement.setString(oneBasedIndex++, feed_lang);
        feedStartDateField.setParameter(statement, oneBasedIndex++, feed_start_date);
        feedEndDateField.setParameter(statement, oneBasedIndex++, feed_end_date);
        statement.setString(oneBasedIndex++, feed_version);
    }

    public static class Loader extends Entity.Loader<FeedInfo> {

        public Loader(GTFSFeed feed) {
            super(feed, "feed_info");
        }

        @Override
        protected boolean isRequired() {
            return false;
        }

        @Override
        public void loadOneRow() throws IOException {
            FeedInfo fi = new FeedInfo();
            fi.id = row + 1; // offset line number by 1 to account for 0-based row index
            fi.feed_id = getStringField("feed_id", false);
            fi.feed_publisher_name = getStringField("feed_publisher_name", true);
            fi.feed_publisher_url = getUrlField("feed_publisher_url", true);
            fi.feed_lang = getStringField("feed_lang", true);
            fi.feed_start_date = getDateField("feed_start_date", false);
            fi.feed_end_date = getDateField("feed_end_date", false);
            fi.feed_version = getStringField("feed_version", false);
            fi.feed = feed;
            if (feed.feedInfo.isEmpty()) {
                feed.feedInfo.put("NONE", fi);
                feed.feedId = fi.feed_id;
            } else {
                feed.errors.add(new GeneralError(tableName, row, null, "FeedInfo contains more than one record."));
            }
        }
    }

    public static class Writer extends Entity.Writer<FeedInfo> {

        public Writer(GTFSFeed feed) {
            super(feed, "feed_info");
        }

        @Override
        public void writeHeaders() throws IOException {
            writer.writeRecord(new String[] {"feed_id", "feed_publisher_name", "feed_publisher_url", "feed_lang",
                    "feed_start_date", "feed_end_date", "feed_version"});
        }

        @Override
        public void writeOneRow(FeedInfo i) throws IOException {
            writeStringField(i.feed_id != null && i.feed_id.equals("NONE") ? "" : i.feed_id);
            writeStringField(i.feed_publisher_name);
            writeUrlField(i.feed_publisher_url);
            writeStringField(i.feed_lang);

            if (i.feed_start_date != null) writeDateField(i.feed_start_date);
            else writeStringField("");

            if (i.feed_end_date != null) writeDateField(i.feed_end_date);
            else writeStringField("");

            writeStringField(i.feed_version);
            endRecord();
        }

        @Override
        public Iterator<FeedInfo> iterator() {
            return feed.feedInfo.values().iterator();
        }
    }
}
