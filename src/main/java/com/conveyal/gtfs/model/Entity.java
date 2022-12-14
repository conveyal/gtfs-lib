package com.conveyal.gtfs.model;

import com.beust.jcommander.internal.Sets;
import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.error.DateParseError;
import com.conveyal.gtfs.error.EmptyFieldError;
import com.conveyal.gtfs.error.EmptyTableError;
import com.conveyal.gtfs.error.GeoJsonParseError;
import com.conveyal.gtfs.error.MissingColumnError;
import com.conveyal.gtfs.error.MissingTableError;
import com.conveyal.gtfs.error.NumberParseError;
import com.conveyal.gtfs.error.RangeError;
import com.conveyal.gtfs.error.ReferentialIntegrityError;
import com.conveyal.gtfs.error.TableInSubdirectoryError;
import com.conveyal.gtfs.error.TimeParseError;
import com.conveyal.gtfs.error.URLParseError;
import com.conveyal.gtfs.loader.DateField;
import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.util.Deduplicator;
import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import static com.conveyal.gtfs.loader.Table.LOCATION_GEO_JSON_FILE_NAME;

/**
 * An abstract base class that represents a row in a GTFS table, e.g. a Stop, Trip, or Agency.
 * One concrete subclass is defined for each table in a GTFS feed.
 */
// TODO K is the key type for this table
public abstract class Entity implements Serializable {

    private static final long serialVersionUID = -3576441868127607448L;
    public static final int INT_MISSING = Integer.MIN_VALUE;
    public static final double DOUBLE_MISSING = Double.MIN_VALUE;
    /** Represents the csv line for feeds that have been loaded from a zip file. Otherwise it is simply a unique ID. */
    public int id;

    /* The feed from which this entity was loaded. TODO is this really necessary in every entity? */
    transient GTFSFeed feed;

    /**
     * This method should be overridden by each Entity subtype to return the proper key field for that subtype.
     * @return a key that according to the GTFS spec should uniquely identify this entity, either alone or together
     *          with a sequence number. For example stop_times and shapes have no single field that uniquely
     *          identifies a row, and the stop_sequence or shape_pt_sequence must also be considered.
     */
    public String getId () {
        return null;
    }

    /**
     * This method should be overridden by each Entity subtype to return the proper sequence field for that subtype.
     * @return the integer second element of a compound key, for those elements that require one.
     */
    public Integer getSequenceNumber () {
        return null;
    }

    /**
     * This method should be overridden by each Entity subtybe to handle setting parameters for a prepared statement
     * created from the entity table constants defined in {@link com.conveyal.gtfs.loader.Table}.
     */
    public abstract void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException;

    public static void setIntParameter (PreparedStatement statement, int oneBasedIndex, int value) throws SQLException {
        if (value == INT_MISSING) statement.setNull(oneBasedIndex, JDBCType.INTEGER.getVendorTypeNumber());
        else statement.setInt(oneBasedIndex, value);
    }

    public static void setDoubleParameter (PreparedStatement statement, int oneBasedIndex, double value) throws SQLException {
        if (value == DOUBLE_MISSING) statement.setNull(oneBasedIndex, JDBCType.DOUBLE.getVendorTypeNumber());
        else statement.setDouble(oneBasedIndex, value);
    }

    /* A class that can produce Entities from CSV, and record errors that occur in the process. */
    // This is almost a GTFSTable... rename?
    public static abstract class Loader<E extends Entity> {

        private static final Logger LOG = LoggerFactory.getLogger(Loader.class);
        private static final Deduplicator deduplicator = new Deduplicator();

        protected final GTFSFeed feed;    // the feed into which we are loading the entities
        protected final String tableName; // name of corresponding table without .txt
        protected final Set<String> missingRequiredColumns = Sets.newHashSet();

        protected CsvReader reader;
        protected int       row;
        // TODO "String column" that is set before any calls to avoid passing around the column name

        public Loader(GTFSFeed feed, String tableName) {
            this.feed = feed;
            this.tableName = tableName;
        }

        /** @return whether the number actual is in the range [min, max] */
        protected boolean checkRangeInclusive(double min, double max, double actual) {
            if (actual < min || actual > max) {
                feed.errors.add(new RangeError(tableName, row, null, min, max, actual)); // TODO set column name in loader so it's available in methods
                return false;
            }
            return true;
        }

        /**
         * Fetch the value from the given column of the current row. Record an error the first time a column is
         * seen to be missing, and whenever empty values are encountered.
         * I was originally just calling getStringField from the other getXField functions as a first step to get
         * the missing-field check. But we don't want deduplication performed on strings that aren't being retained.
         * Therefore the missing-field behavior is this separate function.
         * @return null if column was missing or field is empty
         */
        private String getFieldCheckRequired (String column, boolean required) throws IOException {
            String str = reader.get(column);
            if (str == null) {
                if (!missingRequiredColumns.contains(column)) {
                    feed.errors.add(new MissingColumnError(tableName, column));
                    missingRequiredColumns.add(column);
                }
            } else if (str.isEmpty()) {
                if (required) {
                    feed.errors.add(new EmptyFieldError(tableName, row, column));
                }
                str = null;
            }
            return str;
        }

        /** @return the given column from the current row as a deduplicated String. */
        protected String getStringField (String column, boolean required) throws IOException {
            String str = getFieldCheckRequired(column, required);
            str = deduplicator.deduplicateString(str);
            return str;
        }

        protected int getIntField (String column, boolean required, int min, int max) throws IOException {
            return getIntField(column, required, min, max, 0);
        }

        protected int getIntField (String column, boolean required, int min, int max, int defaultValue) throws IOException {
            String str = getFieldCheckRequired(column, required);
            int val = INT_MISSING;
            if (str == null) {
                val = defaultValue; // defaults to 0 per overloaded function, unless provided.
            } else try {
                val = Integer.parseInt(str);
                checkRangeInclusive(min, max, val);
            } catch (NumberFormatException nfe) {
                feed.errors.add(new NumberParseError(tableName, row, column));
            }
            return val;
        }

        /**
         * Fetch the given column of the current row, and interpret it as a time in the format HH:MM:SS.
         * @return the time value in seconds since midnight
         */
        protected int getTimeField(String column, boolean required) throws IOException {
            String str = getFieldCheckRequired(column, required);
            int val = INT_MISSING;
            
            if (str != null) {
                String[] fields = str.split(":");
                if (fields.length != 3) {
                    feed.errors.add(new TimeParseError(tableName, row, column));
                } else {
                    try {
                        int hours = Integer.parseInt(fields[0]);
                        int minutes = Integer.parseInt(fields[1]);
                        int seconds = Integer.parseInt(fields[2]);
                        checkRangeInclusive(0, 72, hours); // GTFS hours can go past midnight. Some trains run for 3 days.
                        checkRangeInclusive(0, 59, minutes);
                        checkRangeInclusive(0, 59, seconds);
                        val = (hours * 60 * 60) + minutes * 60 + seconds;
                    } catch (NumberFormatException nfe) {
                        feed.errors.add(new TimeParseError(tableName, row, column));
                    }
                }
            }
            
            return val;
        }

        /**
         * Fetch the given column of the current row, and interpret it as a date in the format YYYYMMDD.
         * While Postgres has a date type, SQLite does not have any date types, let alone a specific timezone free one.
         * @return the date value as Java LocalDate, or null if it could not be parsed.
         */
        protected LocalDate getDateField(String column, boolean required) throws IOException {
            String str = getFieldCheckRequired(column, required);
            LocalDate dateTime = null;
            if (str != null) try {
                dateTime = LocalDate.parse(str, DateField.GTFS_DATE_FORMATTER);
                checkRangeInclusive(2000, 2100, dateTime.getYear());
            } catch (IllegalArgumentException | DateTimeParseException e) {
                feed.errors.add(new DateParseError(tableName, row, column));
            }
            return dateTime;
        }

        /**
         * Fetch the given column of the current row, and interpret it as a URL.
         * @return the URL, or null if the field was missing or empty.
         */
        protected URL getUrlField(String column, boolean required) throws IOException {
            String str = getFieldCheckRequired(column, required);
            URL url = null;
            if (str != null) try {
                url = new URL(str);
            } catch (MalformedURLException mue) {
                feed.errors.add(new URLParseError(tableName, row, column));
            }
            return url;
        }

        protected double getDoubleField(String column, boolean required, double min, double max) throws IOException {
            String str = getFieldCheckRequired(column, required);
            double val = Double.NaN;
            if (str != null) try {
                val = Double.parseDouble(str);
                checkRangeInclusive(min, max, val);
            } catch (NumberFormatException nfe) {
                feed.errors.add(new NumberParseError(tableName, row, column));
            }
            return val;
        }

        /**
         * Used to check referential integrity.
         * Return value is not used, but could allow entities to point to each other directly rather than
         * using indirection through string-keyed maps.
         */
        protected <K, V> V getRefField(String column, boolean required, Map<K, V> target) throws IOException {
            String str = getFieldCheckRequired(column, required);
            V val = null;
            if (str != null) {
                val = target.get(str);
                String transitId = column + ":" + str;
                if (!feed.transitIds.contains(transitId)) {
                    feed.transitIds.add(transitId);
                    if (val == null) {
                        feed.errors.add(new ReferentialIntegrityError(tableName, row, column, str));
                    }
                }
            }
            return val;
        }

        protected abstract boolean isRequired();

        /** Implemented by subclasses to read one row, produce one GTFS entity, and store that entity in a map. */
        protected abstract void loadOneRow() throws IOException;

        /**
         * The main entry point into an Entity.Loader. Interprets each row of a CSV file within a zip file as a sinle
         * GTFS entity, and loads them into a table.
         *
         * @param zip the zip file from which to read a table
         */
        public void loadTable(ZipFile zip) throws IOException {
            String tableFileName = tableName + ".txt";
            if (tableName.equals(Table.LOCATIONS.name) || tableName.equals(Table.LOCATION_SHAPES.name)) {
                tableFileName = LOCATION_GEO_JSON_FILE_NAME;
                LOG.info("Loading data for {}, into supporting table {}", tableFileName, tableName);
            }
            ZipEntry entry = zip.getEntry(tableFileName);
            if (entry == null) {
                Enumeration<? extends ZipEntry> entries = zip.entries();
                // check if table is contained within sub-directory
                while (entries.hasMoreElements()) {
                    ZipEntry e = entries.nextElement();
                    if (e.getName().endsWith(tableName + ".txt")) {
                        entry = e;
                        feed.errors.add(new TableInSubdirectoryError(tableName, entry.getName().replace(tableFileName, "")));
                    }
                }
                /* This GTFS table did not exist in the zip. */
                if (this.isRequired()) {
                    feed.errors.add(new MissingTableError(tableName));
                } else {
                    LOG.info("Table {} was missing but it is not required.", tableName);
                }

                if (entry == null) return;
            }
            LOG.info("Loading GTFS table {} from {}", tableName, entry);
            List<String> geoJsonErrors = new ArrayList<>();
            this.reader = Table.getCsvReader(tableFileName, tableName, zip, entry, geoJsonErrors);
            if (!geoJsonErrors.isEmpty()) {
                geoJsonErrors.forEach(error -> feed.errors.add(new GeoJsonParseError(tableName, error)));
            }
            boolean hasHeaders = reader.readHeaders();
            if (!hasHeaders) {
                feed.errors.add(new EmptyTableError(tableName));
            }
            while (reader.readRecord()) {
                // reader.getCurrentRecord() is zero-based and does not include the header line, keep our own row count
                if (++row % 500000 == 0) {
                    LOG.info("Record number {}", human(row));
                }
                loadOneRow(); // Call subclass method to produce an entity from the current row.
            }
            if (row == 0) {
                feed.errors.add(new EmptyTableError(tableName));
            }
        }

    }

    /**
     * An output stream that cannot be closed. CSVWriters try to close their output streams when they are garbage-collected,
     * which breaks if another CSV writer is still writing to the ZIP file.
     *
     * Apache Commons has something similar but it seemed silly to import another large dependency. Eventually Guava will have this,
     * see Guava issue 1367. At that point we should switch to using Guava.
     */
    private static class UncloseableOutputStream extends FilterOutputStream {
        public UncloseableOutputStream(OutputStream out) {
            super(out);
        }

        @Override
        public void close () {
            // no-op
            return;
        }
    }

    /**
     * Write this entity to a CSV file. This should be subclassed in subclasses of Entity.
     * The following (abstract) methods should be overridden in a subclass:
     * 
     * writeHeaders(): write the headers to the CsvWriter writer.
     * writeRow(E): write the passed-in object to the CsvWriter writer, potentially using the write*Field methods.
     * iterator(): return an iterator over objects of this class (note that the feed is available at this.feed
     * public Writer (GTFSFeed feed): this should super to Writer(GTFSFeed feed, String tableName), with the table name
     * defined. 
     * 
     * @author mattwigway
     */
    public static abstract class Writer<E extends Entity> {
        private static final Logger LOG = LoggerFactory.getLogger(Writer.class);

        protected final GTFSFeed feed;    // the feed into which we are loading the entities
        protected final String tableName; // name of corresponding table without .txt

        protected CsvWriter writer;

        /**
         * one-based to match reader.
         */
        protected long row;

        protected Writer(GTFSFeed feed, String tableName) {
            this.feed = feed;
            this.tableName = tableName;
        }

        /**
         * Write the CSV header.
         */
        protected abstract void writeHeaders() throws IOException;

        /**
         * Write one row of the CSV from the passed-in object.
         */
        protected abstract void writeOneRow(E obj) throws IOException;

        /**
         * Get an iterator over objects of this type.
         */
        protected abstract Iterator<E> iterator();

        public void writeTable (ZipOutputStream zip) throws IOException {
            LOG.info("Writing GTFS table {}", tableName);

            ZipEntry zipEntry = new ZipEntry(tableName + ".txt");
            zip.putNextEntry(zipEntry);

            // don't let CSVWriter close the stream when it is garbage-collected
            OutputStream protectedOut = new UncloseableOutputStream(zip);
            this.writer = new CsvWriter(protectedOut, ',', Charset.forName("UTF8"));

            this.writeHeaders();

            // write rows until there are none left.
            row = 0;        	
            Iterator<E> iter = this.iterator();
            while (iter.hasNext()) {
                if (++row % 500000 == 0) {
                    LOG.info("Record number {}", human(row));
                }

                writeOneRow(iter.next());
            }

            // closing the writer closes the underlying output stream, so we don't do that.
            writer.flush();
            zip.closeEntry();

            LOG.info("Wrote {} rows", human(row));
        }

        protected void writeStringField(String str) throws IOException {
            writer.write(str);
        }

        protected void writeUrlField(URL obj) throws IOException {
            writeStringField(obj != null ? obj.toString() : "");
        }

        /**
         * Writes date as YYYYMMDD
         */
        protected void writeDateField (LocalDate d) throws IOException {
            writeStringField(d.format(DateTimeFormatter.BASIC_ISO_DATE));
        }

        /**
         * Take a time expressed in seconds since noon - 12h (midnight, usually) and write it in HH:MM:SS format.
         */
        protected void writeTimeField (int secsSinceMidnight) throws IOException {
            if (secsSinceMidnight == INT_MISSING) {
                writeStringField("");
                return;
            }
            
            writeStringField(convertToGtfsTime(secsSinceMidnight));
        }

        public static String convertToGtfsTime (int secsSinceMidnight) {
            int seconds = secsSinceMidnight % 60;
            secsSinceMidnight -= seconds;
            // note that the minute and hour values are still expressed in seconds until we write it out, to avoid unnecessary division.
            int minutes = (secsSinceMidnight % 3600);
            // secsSinceMidnight now represents hours
            secsSinceMidnight -= minutes;

            // integer divide is fine as we've subtracted off remainders
            return String.format("%02d:%02d:%02d", secsSinceMidnight / 3600, minutes / 60, seconds);
        }

        protected void writeIntField (Integer val) throws IOException {
            if (val.equals(INT_MISSING))
                writeStringField("");
            else
                writeStringField(val.toString());
        }

        /**
         * Write a double value, with precision 10^-7. NaN is written as "".
         */
        protected void writeDoubleField (double val) throws IOException {
            // NaN's represent missing values
            if (Double.isNaN(val))
                writeStringField("");
            
            // control file size: don't use unnecessary precision
            // This is usually used for coordinates; one ten-millionth of a degree at the equator is 1.1cm,
            // and smaller elsewhere on earth, plenty precise enough.
            // On Jupiter, however, it's a different story.
            // Use the US locale so that . is used as the decimal separator
            else
                writeStringField(String.format(Locale.US, "%.7f", val));
        }

        /**
         * End a row.
         * This is just a proxy to the writer, but could be used for hooks in the future.
         */
        public void endRecord () throws IOException {
            writer.endRecord();
        }
    }

    // shared code between reading and writing
    public static final String human (long n) {
        if (n >= 1000000) return String.format("%.1fM", n/1000000.0);
        if (n >= 1000) return String.format("%.1fk", n/1000.0);
        else return String.format("%d", n);
    }

    public static int getIntValue(String value) {
        return value == null ? INT_MISSING : Integer.parseInt(value);
    }

    public static double getDoubleValue(String value) {
        return value == null ? DOUBLE_MISSING : Double.parseDouble(value);
    }

}
