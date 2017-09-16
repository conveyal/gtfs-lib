package com.conveyal.gtfs;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

public class GTFSMainTest {
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();

    @Before
    public void setUpStreams() {
        System.setOut(new PrintStream(outContent));
    }

    @Test
    public void canPrintHelp() throws Exception {
        String[] args = {"-help"};
        GTFSMain.main(args);
        assertThat(outContent.toString(), containsString("usage: java"));
    }

    @Test
    public void exitsIfNoArgumentsProvided() throws Exception {
        exit.expectSystemExitWithStatus(1);
        String[] args = {};
        GTFSMain.main(args);
        assertThat(outContent.toString(), containsString("usage: java"));
    }

    @Test
    public void canValidateSimpleAgency() throws Exception {
        String[] args = {"./src/test/resources/fake-agency.zip", "-validate"};
        GTFSMain.main(args);
    }
}
