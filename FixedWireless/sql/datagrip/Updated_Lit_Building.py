
def build_lit_building_query(longitude, latitude, distance, limit):
    lit_query = f"""
    select
        'ospi' as candidate_type,
        buildingid::text,
        round((st_distance(st_geomfromtext('POINT({longitude} {latitude})', 4326)::geography,location::geography)/1000)::numeric, 2) as cand_dist_km,
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
    
    where st_distance(st_geomfromtext('POINT({longitude} {latitude})', 4326)::geography,location::geography) < ({distance}*1000)
    order by cand_dist_km limit {limit}
    """

    return lit_query
