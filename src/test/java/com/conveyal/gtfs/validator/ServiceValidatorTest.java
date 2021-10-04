package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.model.Calendar;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;

public class ServiceValidatorTest {
    @Test
    public void validateCalendarDays() {
        Calendar calendar = new Calendar();
        assertThat(ServiceValidator.isCalendarUsedDuringWeek(calendar), CoreMatchers.is(false));

        calendar.tuesday = 1;
        assertThat(ServiceValidator.isCalendarUsedDuringWeek(calendar), CoreMatchers.is(true));
    }
}
