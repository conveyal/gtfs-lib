package com.conveyal.gtfs;

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * A test suite for the GTFS Class
 */
public class GTFSTest {
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();

    // setup a stream to capture the output from the program
    @Before
    public void setUpStreams() {
        System.setOut(new PrintStream(outContent));
    }

    /**
     * Make sure that help can be printed.
     *
     * @throws Exception
     */
    @Test
    public void canPrintHelp() throws Exception {
        String[] args = {"-help"};
        GTFS.main(args);
        assertThat(outContent.toString(), containsString("usage: java"));
    }

    /**
     * Make sure that help is printed if no recognizable arguments are provided.
     *
     * @throws Exception
     */
    @Test
    public void handlesUnknownArgs() throws Exception {
        String[] args = {"-blah"};
        GTFS.main(args);
        assertThat(outContent.toString(), containsString("usage: java"));
    }

    /**
     * Make sure that help is printed if not enough key arguments are provided.
     *
     * @throws Exception
     */
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
     * A helper method that will run GTFS.main with a certain zip file.
     * This tests whether a GTFS zip file can be loaded without any errors.
     *
     * @param zipFileName
     */
    private void runSimpleIntegrationTest(String zipFileName) {
        String newDBName = TestUtils.generateNewDB();
        assertThat(newDBName, not(nullValue()));

        try {
            String[] args = {
                "-load",
                TestUtils.getResourceFileName(zipFileName),
                "-d", "jdbc:postgresql://localhost/" + newDBName,
                "-validate"
            };
            GTFS.main(args);
        } finally {
            TestUtils.dropDB(newDBName);
        }
    }
}
