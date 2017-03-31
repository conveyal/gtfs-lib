-- insert_stop
insert into stops (
    stop_id,
    stop_code,
    stop_name,
    stop_desc,
    stop_lat,
    stop_lon,
    zone_id,
    stop_url,
    location_type,
    parent_station,
    stop_timezone,
    wheelchair_boarding
) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- insert_route
insert into routes (
    route_id,
    agency_id,
    route_short_name,
    route_long_name,
    route_desc,
    route_type,
    route_url,
    route_color,
    route_text_color,
    route_branding_url
) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
