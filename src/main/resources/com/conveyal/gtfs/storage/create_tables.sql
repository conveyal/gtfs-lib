
create table stops (
    stop_id varchar,
    stop_code varchar,
    stop_name varchar,
    stop_desc varchar,
    stop_lat double precision,
    stop_lon double precision,
    zone_id varchar,
    stop_url varchar,
    location_type smallint,
    parent_station varchar,
    stop_timezone varchar,
    wheelchair_boarding smallint
);

create table routes (
    route_id varchar,
    agency_id varchar,
    route_short_name varchar,
    route_long_name varchar,
    route_desc varchar,
    route_type smallint,
    route_url varchar,
    route_color varchar,
    route_text_color varchar,
    route_branding_url varchar
);