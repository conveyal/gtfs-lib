package com.conveyal.gtfs.validator;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class RouteTypeValidatorTest {
    private static final List<Integer> ROUTE_TYPES = Lists.newArrayList(3, 101);
    private static final RouteTypeValidator ROUTE_TYPE_VALIDATOR = new RouteTypeValidator(ROUTE_TYPES);

    @Test
    void validateRouteType() {
        assertThat(ROUTE_TYPE_VALIDATOR.isRouteTypeValid(3), is(true)); // Bus
        assertThat(ROUTE_TYPE_VALIDATOR.isRouteTypeValid(36), is(false)); // Invalid
        assertThat(ROUTE_TYPE_VALIDATOR.isRouteTypeValid(101), is(true)); // High Speed Rail
    }
}
