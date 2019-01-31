package com.conveyal.gtfs.util;

// TODO add fields
public class ShapePointDTO {
    public Integer point_type;
    public Double shape_dist_traveled;
    public String shape_id;
    public Double shape_pt_lat;
    public Double shape_pt_lon;
    public Integer shape_pt_sequence;

    /** Empty constructor for deserialization */
    public ShapePointDTO() {}

    public ShapePointDTO(Integer point_type, Double shape_dist_traveled, String shape_id, Double shape_pt_lat, Double shape_pt_lon, Integer shape_pt_sequence) {
        this.point_type = point_type;
        this.shape_dist_traveled = shape_dist_traveled;
        this.shape_id = shape_id;
        this.shape_pt_lat = shape_pt_lat;
        this.shape_pt_lon = shape_pt_lon;
        this.shape_pt_sequence = shape_pt_sequence;
    }
}
