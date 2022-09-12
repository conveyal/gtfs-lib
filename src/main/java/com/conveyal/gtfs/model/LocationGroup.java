package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;
import com.csvreader.CsvReader;
import org.apache.commons.io.input.BOMInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class LocationGroup extends Entity {

    private static final Logger LOG = LoggerFactory.getLogger(LocationGroup.class);
    private static final long serialVersionUID = -4961539668114167098L;
    private static final int NUMBER_OF_HEADERS = 3;
    private static final int NUMBER_OF_COLUMNS = 3;
    private static final String CSV_HEADER = "location_group_id,location_id,location_group_name" + System.lineSeparator();

    public String location_group_id;
    /**
     * A comma separated list of ids referencing stops.stop_id or id from locations.geojson. These are grouped by
     * {@link LocationGroup#getCsvReader(ZipFile, ZipEntry, List)}.
     */
    public String location_id;
    public String location_group_name;

    public LocationGroup() {
    }

    public LocationGroup(String locationGroupId, String locationId, String locationGroupName) {
        this.location_group_id = locationGroupId;
        this.location_id = locationId;
        this.location_group_name = locationGroupName;
    }

    @Override
    public String getId() {
        return location_group_id;
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#LOCATION_GROUPS}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, location_group_id);
        statement.setString(oneBasedIndex++, location_id);
        statement.setString(oneBasedIndex++, location_group_name);
    }

    public static class Loader extends Entity.Loader<LocationGroup> {

        public Loader(GTFSFeed feed) {
            super(feed, "location_groups");
        }

        @Override
        protected boolean isRequired() {
            return false;
        }

        @Override
        public void loadOneRow() throws IOException {
            LocationGroup locationGroup = new LocationGroup();
            locationGroup.id = row + 1; // offset line number by 1 to account for 0-based row index
            locationGroup.location_group_id = getStringField("location_group_id", true);
            locationGroup.location_id = getStringField("location_id", false);
            locationGroup.location_group_name = getStringField("location_group_name", false);
            // Attempting to put a null key or value will cause an NPE in BTreeMap
            if (locationGroup.location_group_id != null) {
                feed.locationGroups.put(locationGroup.location_group_id, locationGroup);
            }
        }
    }

    public static class Writer extends Entity.Writer<LocationGroup> {
        public Writer(GTFSFeed feed) {
            super(feed, "location_groups");
        }

        @Override
        public void writeHeaders() throws IOException {
            writer.writeRecord(new String[] {"location_group_id", "location_id", "location_group_name"});
        }

        @Override
        public void writeOneRow(LocationGroup locationGroup) throws IOException {
            writeStringField(locationGroup.location_group_id);
            writeStringField(locationGroup.location_id);
            writeStringField(locationGroup.location_group_name);
            endRecord();
        }

        @Override
        public Iterator<LocationGroup> iterator() {
            return this.feed.locationGroups.values().iterator();
        }
    }

    public String toCsvRow() {
        return String.join(
            ",",
            location_group_id,
            location_id.contains(",") ? "\"" + location_id + "\"" : location_id,
            location_group_name
        ) + System.lineSeparator();
    }

    /**
     * Extract the location groups from file and group by location group id. Multiple rows of location groups with the
     * same location group id will be compressed into a single row with comma separated location ids. This is to allow
     * for easier CRUD by the DT UI.
     *
     * E.g. 1,2,"group 1" and 1,3,"group 1", will become: 1,"2,3","group 1".
     *
     * If there are any issues grouping the location groups or there are no location groups, return the default CSV
     * reader. This is to prevent downstream processing from failing where a CSV reader is expected.
     */
    public static CsvReader getCsvReader(ZipFile zipFile, ZipEntry entry, List<String> errors) {
        CsvReader csvReader = new CsvReader(new StringReader(""));
        int locationGroupIdIndex = 0;
        int locationIdIndex = 1;
        int locationGroupNameIndex = 2;
        HashMap<String, LocationGroup> multiLocationGroups = new HashMap<>();
        try {
            InputStream zipInputStream = zipFile.getInputStream(entry);
            csvReader = new CsvReader(new BOMInputStream(zipInputStream), ',', StandardCharsets.UTF_8);
            csvReader.setSkipEmptyRecords(false);
            csvReader.readHeaders();
            String[] headers = csvReader.getHeaders();
            if (headers.length != NUMBER_OF_HEADERS) {
                String message = String.format(
                    "Wrong number of headers, expected=%d; found=%d in %s.",
                    NUMBER_OF_HEADERS,
                    headers.length,
                    entry.getName()
                );
                LOG.warn(message);
                if (errors != null) errors.add(message);
                return csvReader;
            }
            while (csvReader.readRecord()) {
                int lineNumber = ((int) csvReader.getCurrentRecord()) + 2;
                if (csvReader.getColumnCount() != NUMBER_OF_COLUMNS) {
                    String message = String.format("Wrong number of columns for line number=%d; expected=%d; found=%d.",
                        lineNumber,
                        NUMBER_OF_COLUMNS,
                        csvReader.getColumnCount()
                    );
                    LOG.warn(message);
                    if (errors != null) errors.add(message);
                    continue;
                }
                LocationGroup locationGroup = new LocationGroup(
                    csvReader.get(locationGroupIdIndex),
                    csvReader.get(locationIdIndex),
                    csvReader.get(locationGroupNameIndex)
                );
                if (multiLocationGroups.containsKey(locationGroup.location_group_id)) {
                    // Combine location groups with matching location group ids.
                    LocationGroup multiLocationGroup = multiLocationGroups.get(locationGroup.location_group_id);
                    multiLocationGroup.location_id += "," + locationGroup.location_id;
                } else {
                    multiLocationGroups.put(locationGroup.location_group_id, locationGroup);
                }
            }
        } catch (IOException e) {
            return csvReader;
        }
        return (multiLocationGroups.isEmpty())
            ? csvReader
            : produceCsvPayload(multiLocationGroups);
    }

    /**
     * Convert the multiple location groups back into CSV, with header and return a {@link CsvReader} representation.
     */
    private static CsvReader produceCsvPayload(HashMap<String, LocationGroup> multiLocationGroupIds) {
        StringBuilder csvContent = new StringBuilder();
        csvContent.append(CSV_HEADER);
        multiLocationGroupIds.forEach((key, value) -> csvContent.append(value.toCsvRow()));
        return new CsvReader(new StringReader(csvContent.toString()));
    }

    /**
     * Expand all location groups which have multiple location ids into a single row for each location id. This is to
     * conform with the GTFS Flex standard.
     *
     * E.g. 1,"2,3","group 1", will become: 1,2,"group 1" and 1,3,"group 1".
     *
     */
    public static String packLocationGroups(List<LocationGroup> locationGroups) {
        StringBuilder csvContent = new StringBuilder();
        csvContent.append(CSV_HEADER);
        locationGroups.forEach(locationGroup -> {
            if (!locationGroup.location_id.contains(",")) {
                // Single location id reference.
                csvContent.append(locationGroup.toCsvRow());
            } else {
                for (String locationId : locationGroup.location_id.split(",")) {
                    csvContent.append(String.join(
                        ",",
                        locationGroup.location_group_id,
                        locationId,
                        locationGroup.location_group_name
                    ));
                    csvContent.append(System.lineSeparator());
                }
            }
        });
        return csvContent.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LocationGroup that = (LocationGroup) o;
        return Objects.equals(location_group_id, that.location_group_id) &&
            Objects.equals(location_id, that.location_id) &&
            Objects.equals(location_group_name, that.location_group_name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(location_group_id, location_id, location_group_name);
    }
}
