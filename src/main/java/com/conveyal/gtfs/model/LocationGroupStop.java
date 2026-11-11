package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;
import com.csvreader.CsvReader;

import java.io.IOException;
import java.io.StringReader;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.conveyal.gtfs.util.CsvReaderUtil.hasExpectedNumberOfColumns;

public class LocationGroupStop extends Entity {

    private static final long serialVersionUID = 469687473399554677L;
    public static final int NUMBER_OF_HEADERS = 2;
    private static final int NUMBER_OF_COLUMNS = 2;
    private static final String CSV_HEADER = "location_group_id,stop_id" + System.lineSeparator();

    public String location_group_id;
    /**
     * A comma separated list of ids referencing stops.stop_id or id from locations.geojson. These are grouped by
     * {@link LocationGroupStop#getParsedData(CsvReader, List)}.
     */
    public String stop_id;

    public static final String TABLE_NAME = "location_group_stops";
    public static final String LOCATION_GROUP_ID_NAME = "location_group_id";
    public static final String STOP_ID_NAME = "stop_id";


    public LocationGroupStop() {
    }

    public LocationGroupStop(String locationGroupId, String stopId) {
        this.location_group_id = locationGroupId;
        this.stop_id = stopId;
    }

    @Override
    public String getId() {
        return createPrimaryKey(location_group_id, stop_id);
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#LOCATION_GROUP_STOPS}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, location_group_id);
        statement.setString(oneBasedIndex, stop_id);
    }

    public static class Loader extends Entity.Loader<LocationGroupStop> {

        public Loader(GTFSFeed feed) {
            super(feed, TABLE_NAME);
        }

        @Override
        protected boolean isRequired() {
            return false;
        }

        @Override
        public void loadOneRow() throws IOException {
            LocationGroupStop locationGroupStop = new LocationGroupStop();
            locationGroupStop.id = row + 1; // offset line number by 1 to account for 0-based row index
            locationGroupStop.location_group_id = getStringField(LOCATION_GROUP_ID_NAME, true);
            locationGroupStop.stop_id = getStringField(STOP_ID_NAME, true);
            // Attempting to put a null key or value will cause an NPE in BTreeMap
            if (locationGroupStop.location_group_id != null && locationGroupStop.stop_id != null) {
                feed.locationGroupStops.put(locationGroupStop.getId(), locationGroupStop);
            }
        }
    }

    public static class Writer extends Entity.Writer<LocationGroupStop> {
        public Writer(GTFSFeed feed) {
            super(feed, TABLE_NAME);
        }

        @Override
        public void writeHeaders() throws IOException {
            writer.writeRecord(new String[] {LOCATION_GROUP_ID_NAME, STOP_ID_NAME});
        }

        @Override
        public void writeOneRow(LocationGroupStop locationGroupStop) throws IOException {
            writeStringField(locationGroupStop.location_group_id);
            writeStringField(locationGroupStop.stop_id);
            endRecord();
        }

        @Override
        public Iterator<LocationGroupStop> iterator() {
            return this.feed.locationGroupStops.values().iterator();
        }
    }

    public String toCsvRow() {
        // Underlying call to CsvUtil will add quotes for content with commas.
        return createRow(location_group_id, (stop_id != null) ? stop_id : "");
    }

    /**
     * Extract the location group stops from file and group by location group id. Multiple rows of location groups with
     * the same location group id will be compressed into a single row with comma separated stop ids. This is to allow
     * for easier CRUD by the DT UI.
     * <p>
     * E.g.
     * </p>
     * <p>
     * location_group_1,stop_id_1<br>
     * location_group_1,stop_id_2
     * </p>
     * <p>
     * will become:
     * </p>
     * <p>
     * location_group_1,"stop_id_1,stop_id_2"
     * </p>
     * If any issues are encountered or there are no location group stops, return the default {@link CsvReader}. This is
     * to prevent downstream processing from failing where a {@link CsvReader} is expected.
     */
    public static CsvReader getParsedData(CsvReader csvReader, List<String> errors) {
        int locationGroupIdIndex = 0;
        int stopIdIndex = 1;
        SortedMap<String, LocationGroupStop> multiLocationGroupStops = new TreeMap<>();

        csvReader.setSafetySwitch(false);

        try {
            while (csvReader.readRecord()) {
                if (!hasExpectedNumberOfColumns(csvReader, errors, NUMBER_OF_COLUMNS)) {
                    continue;
                }
                LocationGroupStop locationGroupStop = new LocationGroupStop(
                    csvReader.get(locationGroupIdIndex),
                    csvReader.get(stopIdIndex)
                );
                if (multiLocationGroupStops.containsKey(locationGroupStop.location_group_id)) {
                    // Combine stop areas with matching stop areas ids.
                    LocationGroupStop multiLocationGroupStop = multiLocationGroupStops.get(locationGroupStop.location_group_id);
                    multiLocationGroupStop.stop_id += "," + locationGroupStop.stop_id;
                } else {
                    multiLocationGroupStops.put(locationGroupStop.location_group_id, locationGroupStop);
                }
            }
            // Return a brand new CSV reader because other code after this will parse the headers again.
            return produceCsvPayload(multiLocationGroupStops);
        } catch (IOException e) {
            return csvReader;
        }
    }

    /**
     * Convert the multiple location group stops back into CSV, with header and return a {@link CsvReader} representation.
     */
    private static CsvReader produceCsvPayload(SortedMap<String, LocationGroupStop> multiLocationGroupStops) {
        StringBuilder csvContent = new StringBuilder();
        csvContent.append(CSV_HEADER);
        multiLocationGroupStops.forEach((key, value) -> csvContent.append(value.toCsvRow()));
        return new CsvReader(new StringReader(csvContent.toString()));
    }

    /**
     * Expand all location group stops which have multiple stop ids into a single row for each stop id. This is to
     * conform with the GTFS Flex standard.
     * <p>
     * E.g.
     * </p>
     * <p>
     * location_group_1,"stop_id_2,stop_id_3"
     * </p>
     * <p>
     * will become:
     * </p>
     * <p>
     * location_group_1,stop_id_2
     * location_group_1,stop_id_3
     * </p>
     */
    public static String packLocationGroupStops(List<LocationGroupStop> locationGroupStops) {
        StringBuilder csvContent = new StringBuilder();
        csvContent.append(CSV_HEADER);
        locationGroupStops.forEach(locationGroupStop -> {
            if (locationGroupStop.stop_id == null || !locationGroupStop.stop_id.contains(",")) {
                // Single location id reference.
                csvContent.append(locationGroupStop.toCsvRow());
            } else {
                for (String stopId : locationGroupStop.stop_id.split(",")) {
                    csvContent.append(String.join(
                        ",",
                        locationGroupStop.location_group_id,
                        stopId
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
        LocationGroupStop that = (LocationGroupStop) o;
        return
            Objects.equals(location_group_id, that.location_group_id) &&
            Objects.equals(stop_id, that.stop_id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(location_group_id, stop_id);
    }
}

