package com.conveyal.gtfs.util;

import org.junit.Test;

import static com.conveyal.gtfs.util.Util.human;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class UtilTest {

    @Test
    public void canHumanize() {
        assertThat(human(123), is("123"));
        assertThat(human(1234), is("1k"));
        assertThat(human(1234567), is("1.2M"));
        assertThat(human(1234567890), is("1.2G"));
    }
}
