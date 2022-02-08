-- Manhattan Test Coordinates Longitude: -73.9913778, Latitude: 40.7390831
select
        'ospi' as candidate_type,
        buildingid::text,
        round((st_distance(st_geomfromtext('POINT(-73.9913778 40.7390831)', 4326)::geography,location::geography)/1000)::numeric, 2) as cand_dist_km,
        st_x(vertices.geom) as long,
        st_y(vertices.geom) as lat,
        30::double precision as height
    from
        ospi.ne_dw_buildings o

    join lateral
        (
            select
                x.name,
                (public.cc_cardinal_vertices_from_polygon(st_buffer(x.wkt_geometry,-0.000012))).geom

            from ospi.ne_dw_buildings x

            where st_intersects(location, o.wkt_geometry)

        ) as vertices on o.name = vertices.name

    where st_distance(st_geomfromtext('POINT(-73.9913778 40.7390831)', 4326)::geography,location::geography) < (7*1000) --{distance}
    order by cand_dist_km limit 1000--{limit}