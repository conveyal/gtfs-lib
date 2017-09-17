package com.conveyal.gtfs;

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

public class GTFSTest {
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();

    @Before
    public void setUpStreams() {
        System.setOut(new PrintStream(outContent));
    }

    @Test
    public void canPrintHelp() throws Exception {
        String[] args = {"-help"};
        GTFS.main(args);
        assertThat(outContent.toString(), containsString("usage: java"));
    }

    @Test
    public void handlesUnknownArgs() throws Exception {
        String[] args = {"-blah"};
        GTFS.main(args);
        assertThat(outContent.toString(), containsString("usage: java"));
    }

    @Test
    public void requiresActionCommand() throws Exception {
        String[] args = {"-u", "blah"};
        GTFS.main(args);
        assertThat(outContent.toString(), containsString("usage: java"));
    }

    /**
     * Tests whether or not a super simple 2-stop, 1-route, 1-trip, valid gtfs can be loaded
     */
    @Test
    public void canLoadSimpleAgency() {
        runSimpleIntegrationTest("fake-agency.zip");
    }

    /**
     * Run GTFS.main with a certain zip file.
     *
     * @param zipFileName
     */
    private void runSimpleIntegrationTest(String zipFileName) {
        String newDBName = TestUtils.generateNewDB();
        try {
            String[] args = {
                "-load",
                TestUtils.getResourceFileName(zipFileName),
                "-d", "jdbc:postgresql://localhost/" + newDBName,
                "-u", "gtfs_test",
                "-p", "gtfs_test",
                "-validate"
            };
            GTFS.main(args);
        } finally {
            TestUtils.dropDB(newDBName);
        }
    }
}
