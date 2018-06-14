package com.conveyal.gtfs.util;

import org.junit.Before;
import org.junit.Test;

import static com.conveyal.gtfs.util.Util.ensureValidNamespace;
import static com.conveyal.gtfs.util.Util.human;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

/**
 * A test suite to verify the functionality of methods in the Util class.
 */
public class UtilTest {

    @Before // setup()
    public void before() throws Exception {
        java.util.Locale.setDefault(java.util.Locale.US);
    }

    /**
     * Assert that the human function returns strings that are properly formatted.
     */
    @Test
    public void canHumanize() {
        assertThat(human(123), is("123"));
        assertThat(human(1234), is("1k"));
        assertThat(human(1234567), is("1.2M"));
        assertThat(human(1234567890), is("1.2G"));
    }

    @Test
    public void canEnsureValidNamespace() {
        testNamespace("abc123", false);
        testNamespace("abc_123", false);
        testNamespace("abc_123.", false);
        testNamespace("abc 123", true);
        testNamespace("' OR 1=1;SELECT '1", true);
    }

    private void testNamespace(String namespace, boolean shouldFail) {
        boolean errorThrown = false;
        try {
            ensureValidNamespace(namespace);
        } catch (IllegalStateException e) {
            errorThrown = true;
            if (shouldFail) {
                assertThat(e.getMessage(), equalTo(
                    "Namespace must only have alphanumeric characters or the underscore symbol"
                ));
            } else {
                fail(String.format("ensureValidNamespace provided false positive for value: %s", namespace));
            }
        }
        if (shouldFail && !errorThrown) {
            fail(String.format("ensureValidNamespace failed to throw exception for value: %s", namespace));
        }
    }


}
