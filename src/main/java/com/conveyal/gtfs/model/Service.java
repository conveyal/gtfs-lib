package com.conveyal.gtfs.model;

import com.google.common.collect.Maps;
import java.time.LocalDate;

import java.io.Serializable;
import java.util.Map;

/**
 * This table does not exist in GTFS. It is a join of calendars and calendar_dates on service_id.
 * There should only be one Calendar per service_id. There should only be one calendar_date per tuple of
 * (service_id, date), which means there can be many calendar_dates per service_id.
 */
public class Service implements Serializable {

    public String   service_id;
    public Calendar calendar;
    public Map<LocalDate, CalendarDate> calendar_dates = Maps.newHashMap();

    public Service(String service_id) {
        this.service_id = service_id;
    }

    /**
     * Is this service active on the specified date?
     * @param date
     * @return
     */
    public boolean activeOn (LocalDate date) {
        // first check for exceptions
        CalendarDate exception = calendar_dates.get(date);

        if (exception != null)
            return exception.exception_type == 1;

        else if (calendar == null)
            return false;

        else {
            int gtfsDate = date.getYear() * 10000 + date.getMonthValue() * 100 + date.getDayOfMonth();
            if( calendar.end_date >= gtfsDate && calendar.start_date <= gtfsDate) {
        	switch(date.getDayOfWeek().getValue()) {
        	    case 1: return calendar.monday==1;
        	    case 2: return calendar.tuesday==1;
        	    case 3: return calendar.wednesday==1;
        	    case 4: return calendar.thursday==1;
        	    case 5: return calendar.friday==1;
        	    case 6: return calendar.saturday==1;
        	    case 7: return calendar.sunday==1;
        	}
            }
            return false;
        }
    }
}
