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

-- find_unused_stops
select stops.stop_id from stops
left join (
  select stop_id, count(stop_id) as n from stop_times group by stop_id
) as refs
on stops.stop_id = refs.stop_id
where n is null or n = 0;

-- find_superimposed_stops
create temporary table stops_projected as (
    select stop_id,
    stop_lon * cos(stop_lat) * 111111 as mx,
    stop_lat * 111111 as my
    from stops
);
create index on stops_projected (mx);
create index on stops_projected (my);

select a.stop_id, b.stop_id, sqrt(pow(a.mx - b.mx, 2) + pow(a.my - b.my, 2)) as distance_meters
from stops_projected as a, stops_projected as b
where b.mx < a.mx + 50 and b.mx > a.mx - 50
and b.my < a.my + 50 and b.my > a.my - 50
and b.stop_id > a.stop_id;




