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

public class StopArea extends Entity {

    private static final Logger LOG = LoggerFactory.getLogger(StopArea.class);
    private static final long serialVersionUID = 469687473399554677L;
    private static final int NUMBER_OF_HEADERS = 2;
    private static final int NUMBER_OF_COLUMNS = 2;
    private static final String CSV_HEADER = "area_id,stop_id" + System.lineSeparator();

    public String area_id;
    /**
     * A comma separated list of ids referencing stops.stop_id or id from locations.geojson. These are grouped by
     * {@link StopArea#getCsvReader(ZipFile, ZipEntry, List)}.
     */
    public String stop_id;

    public StopArea() {
    }

    public StopArea(String areaId, String stopId) {
        this.area_id = areaId;
        this.stop_id = stopId;
    }

    @Override
    public String getId() {
        return createId(area_id, stop_id);
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#STOP_AREAS}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, area_id);
        statement.setString(oneBasedIndex, stop_id);
    }

    public static class Loader extends Entity.Loader<StopArea> {

        public Loader(GTFSFeed feed) {
            super(feed, "stop_areas");
        }

        @Override
        protected boolean isRequired() {
            return false;
        }

        @Override
        public void loadOneRow() throws IOException {
            StopArea stopArea = new StopArea();
            stopArea.id = row + 1; // offset line number by 1 to account for 0-based row index
            stopArea.area_id = getStringField("area_id", true);
            stopArea.stop_id = getStringField("stop_id", true);
            // Attempting to put a null key or value will cause an NPE in BTreeMap
            if (stopArea.area_id != null && stopArea.stop_id != null) {
                feed.stopAreas.put(createId(stopArea.area_id, stopArea.stop_id), stopArea);
            }
        }
    }

    public static class Writer extends Entity.Writer<StopArea> {
        public Writer(GTFSFeed feed) {
            super(feed, "stop_areas");
        }

        @Override
        public void writeHeaders() throws IOException {
            writer.writeRecord(new String[] {"area_id", "stop_id"});
        }

        @Override
        public void writeOneRow(StopArea stopArea) throws IOException {
            writeStringField(stopArea.area_id);
            writeStringField(stopArea.stop_id);
            endRecord();
        }

        @Override
        public Iterator<StopArea> iterator() {
            return this.feed.stopAreas.values().iterator();
        }
    }

    public String toCsvRow() {
        return String.join(
            ",",
            area_id,
            (stop_id != null)
                ? stop_id.contains(",") ? "\"" + stop_id + "\"" : stop_id
                : ""
        ) + System.lineSeparator();
    }

    /**
     * Extract the stop areas from file and group by stop id. Multiple rows of stop areas with the
     * same stop id will be compressed into a single row with comma separated stop ids. This is to allow
     * for easier CRUD by the DT UI.
     *
     * E.g. 1,2 and 1,3, will become: 1,"2,3".
     *
     * If there are any issues grouping the stop areas or there are no stop areas, return the default CSV
     * reader. This is to prevent downstream processing from failing where a CSV reader is expected.
     */
    public static CsvReader getCsvReader(ZipFile zipFile, ZipEntry entry, List<String> errors) {
        CsvReader csvReader = new CsvReader(new StringReader(""));
        int stopAreaIdIndex = 0;
        int stopIdIndex = 1;
        HashMap<String, StopArea> multiStopAreas = new HashMap<>();
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
                StopArea stopArea = new StopArea(
                    csvReader.get(stopAreaIdIndex),
                    csvReader.get(stopIdIndex)
                );
                if (multiStopAreas.containsKey(stopArea.area_id)) {
                    // Combine stop areas with matching stop areas ids.
                    StopArea multiStopArea = multiStopAreas.get(stopArea.area_id);
                    multiStopArea.stop_id += "," + stopArea.stop_id;
                } else {
                    multiStopAreas.put(stopArea.area_id, stopArea);
                }
            }
        } catch (IOException e) {
            return csvReader;
        }
        return (multiStopAreas.isEmpty())
            ? csvReader
            : produceCsvPayload(multiStopAreas);
    }

    /**
     * Convert the multiple stop areas back into CSV, with header and return a {@link CsvReader} representation.
     */
    private static CsvReader produceCsvPayload(HashMap<String, StopArea> multiStopAreaIds) {
        StringBuilder csvContent = new StringBuilder();
        csvContent.append(CSV_HEADER);
        multiStopAreaIds.forEach((key, value) -> csvContent.append(value.toCsvRow()));
        return new CsvReader(new StringReader(csvContent.toString()));
    }

    /**
     * Expand all stop areas which have multiple stop ids into a single row for each stop id. This is to
     * conform with the GTFS Flex standard.
     *
     * E.g. 1,"2,3", will become: 1,2 and 1,3.
     *
     */
    public static String packStopAreas(List<StopArea> stopAreas) {
        StringBuilder csvContent = new StringBuilder();
        csvContent.append(CSV_HEADER);
        stopAreas.forEach(stopArea -> {
            if (stopArea.stop_id == null || !stopArea.stop_id.contains(",")) {
                // Single location id reference.
                csvContent.append(stopArea.toCsvRow());
            } else {
                for (String stopId : stopArea.stop_id.split(",")) {
                    csvContent.append(String.join(
                        ",",
                        stopArea.area_id,
                        stopId
                    ));
                    csvContent.append(System.lineSeparator());
                }
            }
        });
        return csvContent.toString();
    }

    private static String createId(String areaId, String stopId) {
        return String.format("%s_%s", areaId, stopId);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StopArea that = (StopArea) o;
        return Objects.equals(area_id, that.area_id) &&
            Objects.equals(stop_id, that.stop_id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(area_id, stop_id);
    }
}

