package com.conveyal.gtfs.dto;

public class ScheduleExceptionDTO {
    public String[] added_service;
    public String[] custom_schedule;
    public String[] dates;
    public Integer exemplar;
    public Integer id;
    public String name;
    public String[] removed_service;

    /** Empty constructor for deserialization */
    public ScheduleExceptionDTO() {}
}
