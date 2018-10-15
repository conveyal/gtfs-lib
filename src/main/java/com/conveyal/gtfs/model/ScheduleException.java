package com.conveyal.gtfs.model;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;

/**
 * Represents an exception to the schedule, which could be "On January 18th, run a Sunday schedule"
 * (useful for holidays), or could be "on June 23rd, run the following services" (useful for things
 * like early subway shutdowns, re-routes, etc.)
 *
 * Unlike the GTFS schedule exception model, we assume that these special calendars are all-or-nothing;
 * everything that isn't explicitly running is not running. That is, creating special service means the
 * user starts with a blank slate.
 *
 * @author mattwigway
 */

public class ScheduleException extends Entity {
    private static final long serialVersionUID = 1L;

    /**
     * If non-null, run service that would ordinarily run on this day of the week.
     * Takes precedence over any custom schedule.
     */
    public ExemplarServiceDescriptor exemplar;

    /** The name of this exception, for instance "Presidents' Day" or "Early Subway Shutdowns" */
    public String name;

    /** The dates of this service exception */
    public List<LocalDate> dates;

    /** A custom schedule. Only used if like == null */
    public List<String> customSchedule;

    public List<String> addedService;

    public List<String> removedService;

    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        // FIXME
    }

    public boolean serviceRunsOn(Calendar calendar) {
        switch (exemplar) {
            case MONDAY:
                return calendar.monday == 1;
            case TUESDAY:
                return calendar.tuesday == 1;
            case WEDNESDAY:
                return calendar.wednesday == 1;
            case THURSDAY:
                return calendar.thursday == 1;
            case FRIDAY:
                return calendar.friday == 1;
            case SATURDAY:
                return calendar.saturday == 1;
            case SUNDAY:
                return calendar.sunday == 1;
            case NO_SERVICE:
                // special case for quickly turning off all service.
                return false;
            case CUSTOM:
                return customSchedule != null && customSchedule.contains(calendar.service_id);
            case SWAP:
                // Exception type which explicitly adds or removes a specific service for the provided dates.
                if (addedService != null && addedService.contains(calendar.service_id)) {
                    return true;
                }
                if (removedService != null && removedService.contains(calendar.service_id)) {
                    return false;
                }
            default:
                // can't actually happen, but java requires a default with a return here
                return false;
        }
    }

    /**
     * Represents a desire about what service should be like on a particular day.
     * For example, run Sunday service on Presidents' Day, or no service on New Year's Day.
     */
    public enum ExemplarServiceDescriptor {
        MONDAY(0), TUESDAY(1), WEDNESDAY(2), THURSDAY(3), FRIDAY(4), SATURDAY(5), SUNDAY(6), NO_SERVICE(7), CUSTOM(8), SWAP(9), MISSING(-1);

        private final int value;

        ExemplarServiceDescriptor(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    public static ExemplarServiceDescriptor exemplarFromInt (int value) {
        switch (value) {
            case 0:
                return ExemplarServiceDescriptor.MONDAY;
            case 1:
                return ExemplarServiceDescriptor.TUESDAY;
            case 2:
                return ExemplarServiceDescriptor.WEDNESDAY;
            case 3:
                return ExemplarServiceDescriptor.THURSDAY;
            case 4:
                return ExemplarServiceDescriptor.FRIDAY;
            case 5:
                return ExemplarServiceDescriptor.SATURDAY;
            case 6:
                return ExemplarServiceDescriptor.SUNDAY;
            case 7:
                return ExemplarServiceDescriptor.NO_SERVICE;
            case 8:
                return ExemplarServiceDescriptor.CUSTOM;
            case 9:
                return ExemplarServiceDescriptor.SWAP;
            default:
                return ExemplarServiceDescriptor.MISSING;
        }
    }
}
