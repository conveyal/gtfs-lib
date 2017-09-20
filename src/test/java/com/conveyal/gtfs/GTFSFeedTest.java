package com.conveyal.gtfs;

import org.junit.Test;

import java.io.File;

import static com.conveyal.gtfs.TestUtils.getResourceFileName;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test suite for the GTFSFeed class.
 */
public class GTFSFeedTest {

    /**
     * Make sure a roundtrip of loading a GTFS zip file and then writing another zip file can be performed.
     */
    @Test
    public void canDoRoundtripLoadAndWriteToZipFile() {
        String outZip = getResourceFileName("fake-agency-output.zip");
        File file = new File(outZip);
        file.delete();
        try {
            GTFSFeed feed = new GTFSFeed();
            feed.fromFile(getResourceFileName("fake-agency.zip"));
            feed.toFile(outZip);
            assertThat(file.exists(), equalTo(true));
            // TODO: assert that rows of data were written
        } finally {
            file.delete();
        }
    }
}
