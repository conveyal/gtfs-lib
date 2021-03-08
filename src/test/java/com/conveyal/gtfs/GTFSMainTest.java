package com.conveyal.gtfs;

import com.ginsberg.junit.exit.ExpectSystemExitWithStatus;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

/**
 * A test suite for the GTFSMain class.
 */
public class GTFSMainTest {
    private static String simpleGtfsZipFileName;
    private static String badGtfsZipFileName;
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();

    @BeforeAll
    public static void setUpClass() {
        //executed only once, before the first test
        simpleGtfsZipFileName = null;
        try {
            simpleGtfsZipFileName = TestUtils.zipFolderFiles("fake-agency", true);
            badGtfsZipFileName = TestUtils.zipFolderFiles("fake-agency-bad-calendar-date", true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // setup a stream to capture the output from the program
    @BeforeEach
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
        GTFSMain.main(args);
        assertThat(outContent.toString(), containsString("usage: java"));
    }

    /**
     * Make sure that help can is printed if no arguments are provided.
     *
     * @throws Exception
     */
    @Test
    @ExpectSystemExitWithStatus(1)
    public void exitsIfNoArgumentsProvided() throws Exception {
        String[] args = {};
        GTFSMain.main(args);
        assertThat(outContent.toString(), containsString("usage: java"));
    }

    /**
     * Verify that a simple GTFS Zip file can be loaded with the GTFSMain class.
     *
     * @throws Exception
     */
    @Test
    public void canValidateSimpleAgency() throws Exception {
        String[] args = {simpleGtfsZipFileName, "-validate"};
        GTFSMain.main(args);
    }

    /**
     * Verify that a simple GTFS Zip file can be loaded with the GTFSMain class.
     *
     * @throws Exception
     */
    @Test
    public void canValidateFeedWithErrors() throws Exception {
        String[] args = {badGtfsZipFileName, "-validate"};
        GTFSMain.main(args);
    }
}
