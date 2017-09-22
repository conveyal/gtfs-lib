package com.conveyal.gtfs;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

/**
 * A test suite for the GTFSMain class
 */
public class GTFSMainTest {
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();

    // this is used so that it is possible to test code that calls System.exit()
    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();

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
        GTFSMain.main(args);
        assertThat(outContent.toString(), containsString("usage: java"));
    }

    /**
     * Make sure that help can is printed if no arguments are provided.
     *
     * @throws Exception
     */
    @Test
    public void exitsIfNoArgumentsProvided() throws Exception {
        exit.expectSystemExitWithStatus(1);
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
        String[] args = {"./src/test/resources/fake-agency.zip", "-validate"};
        GTFSMain.main(args);
    }
}
