package com.conveyal.gtfs.validator;

import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class MTCValidatorTest {
    @Test
    public void canValidateFieldLength() {
        MTCValidator validator = new MTCValidator(null, null);
        assertThat(validator.validateFieldLength(null, "abcdefghijklmnopqrstwxyz1234567890", 20), is(false));
        assertThat(validator.validateFieldLength(null, "abcdef", 20), is(true));
    }

    @Test
    public void canValidateFieldLength_usingObject() throws MalformedURLException {
        MTCValidator validator = new MTCValidator(null, null);

        // You can also pass objects, in that case it will use toString().
        URL url = new URL("http://www.gtfs.org");
        assertThat(validator.validateFieldLength(null, url, 10), is(false));
        assertThat(validator.validateFieldLength(null, url, 30), is(true));
    }
}
