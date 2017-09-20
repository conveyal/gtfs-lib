package com.conveyal.gtfs;

import com.csvreader.CsvReader;
import org.apache.commons.io.input.BOMInputStream;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.conveyal.gtfs.TestUtils.getResourceFileName;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test suite for the GTFSFeed class.
 */
public class GTFSFeedTest {

    private static final Logger LOG = LoggerFactory.getLogger(GTFSFeedTest.class);

    /**
     * Make sure a roundtrip of loading a GTFS zip file and then writing another zip file can be performed.
     */
    @Test
    public void canDoRoundtripLoadAndWriteToZipFile() throws IOException {
        String outZip = getResourceFileName("fake-agency-output.zip");
        File file = new File(outZip);
        file.delete();
        try {
            GTFSFeed feed = new GTFSFeed();
            feed.fromFile(getResourceFileName("fake-agency.zip"));
            feed.toFile(outZip);
            assertThat(file.exists(), equalTo(true));

            // assert that rows of data were written to files within the zipfile
            ZipFile zip = new ZipFile(file);

            FileTestCase[] fileTestCases = {
                // agency.txt
                new FileTestCase(
                    "agency.txt",
                    new DataExpectation[]{
                        new DataExpectation("agency_id", "1"),
                        new DataExpectation("agency_name", "Fake Transit")
                    }
                ),
                new FileTestCase(
                    "calendar.txt",
                    new DataExpectation[]{
                        new DataExpectation("service_id", "04100312-8fe1-46a5-a9f2-556f39478f57"),
                        new DataExpectation("start_date", "20170915"),
                        new DataExpectation("end_date", "20170917")
                    }
                ),
                new FileTestCase(
                    "routes.txt",
                    new DataExpectation[]{
                        new DataExpectation("agency_id", "1"),
                        new DataExpectation("route_id", "1"),
                        new DataExpectation("route_long_name", "Route 1")
                    }
                ),
                new FileTestCase(
                    "shapes.txt",
                    new DataExpectation[]{
                        new DataExpectation("shape_id", "5820f377-f947-4728-ac29-ac0102cbc34e"),
                        new DataExpectation("shape_pt_lat", "37.0612132"),
                        new DataExpectation("shape_pt_lon", "-122.0074332")
                    }
                ),
                new FileTestCase(
                    "stop_times.txt",
                    new DataExpectation[]{
                        new DataExpectation("trip_id", "a30277f8-e50a-4a85-9141-b1e0da9d429d"),
                        new DataExpectation("departure_time", "07:00:00"),
                        new DataExpectation("stop_id", "4u6g")
                    }
                ),
                new FileTestCase(
                    "trips.txt",
                    new DataExpectation[]{
                        new DataExpectation("route_id", "1"),
                        new DataExpectation("trip_id", "a30277f8-e50a-4a85-9141-b1e0da9d429d"),
                        new DataExpectation("service_id", "04100312-8fe1-46a5-a9f2-556f39478f57")
                    }
                )
            };


            for (FileTestCase fileTestCase: fileTestCases) {
                ZipEntry entry = zip.getEntry(fileTestCase.filename);
                assertThat(entry, notNullValue());
                InputStream zis = zip.getInputStream(entry);
                InputStream bis = new BOMInputStream(zis);
                CsvReader reader = new CsvReader(bis, ',', Charset.forName("UTF8"));
                boolean hasHeaders = reader.readHeaders();
                assertThat(hasHeaders, is(true));
                reader.readRecord();
                for (DataExpectation dataExpectation: fileTestCase.expectedColumnData) {
                    assertThat(reader.get(dataExpectation.columnName), equalTo(dataExpectation.expectedValue));
                }
            }
        } finally {
            file.delete();
        }
    }
}

class FileTestCase {
    public String filename;
    public DataExpectation[] expectedColumnData;

    public FileTestCase(String filename, DataExpectation[] expectedColumnData) {
        this.filename = filename;
        this.expectedColumnData = expectedColumnData;
    }
}

class DataExpectation {
    public String columnName;
    public String expectedValue;

    public DataExpectation(String columnName, String expectedValue) {
        this.columnName = columnName;
        this.expectedValue = expectedValue;
    }
}