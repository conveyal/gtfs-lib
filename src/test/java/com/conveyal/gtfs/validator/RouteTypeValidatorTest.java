package com.conveyal.gtfs.validator;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

class RouteTypeValidatorTest {
    private static final List<Integer> TEST_ROUTE_TYPES = Lists.newArrayList(3, 101);
    private static final RouteTypeValidator ROUTE_TYPE_VALIDATOR = new RouteTypeValidator(TEST_ROUTE_TYPES);

    @Test
    void validateRouteType() {
        assertTrue(ROUTE_TYPE_VALIDATOR.isRouteTypeValid(3)); // Bus
        assertTrue(ROUTE_TYPE_VALIDATOR.isRouteTypeValid(36)); // Invalid
        assertTrue(ROUTE_TYPE_VALIDATOR.isRouteTypeValid(101)); // High Speed Rail
    }
}
