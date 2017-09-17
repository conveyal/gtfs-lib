package com.conveyal.gtfs;

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

public class GTFSTest {
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();

    String resourcesDir = "./src/test/resources/";

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

    @Test
    public void canLoadSimpleAgency() {
        String[] args = {
            "-load",
            resourcesDir + "fake-agency.zip",
            "-d", "jdbc:postgresql://localhost/gtfs_lib_test",
            "-u", "gtfs_test",
            "-p", "gtfs_test"
        };
        GTFS.main(args);
    }
}
