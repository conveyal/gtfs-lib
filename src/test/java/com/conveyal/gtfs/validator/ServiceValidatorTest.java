package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.model.Calendar;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ServiceValidatorTest {
    @Test
    public void validateCalendarDays() {
        Calendar calendar = new Calendar();
        assertThat(ServiceValidator.isCalendarUsedDuringWeek(calendar), is(false));

        calendar.tuesday = 1;
        assertThat(ServiceValidator.isCalendarUsedDuringWeek(calendar), is(true));
    }
}
