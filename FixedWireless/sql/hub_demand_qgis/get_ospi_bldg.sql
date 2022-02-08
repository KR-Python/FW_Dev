SELECT
        ospi_bldg.buildingid,
        ospi_bldg.name,
        ospi_bldg.building_type,
        crm_bldg.on_net_status,
        ST_y(ospi_bldg.location) as "Lat Dec",
        ST_x(ospi_bldg.location) as "Long Dec"
    FROM ospi.ne_dw_buildings AS ospi_bldg

    LEFT JOIN crm.crm_dw_buildings AS crm_bldg ON TRIM(UPPER(ospi_bldg.name)) = crm_bldg.clli

WHERE (crm_bldg.on_net_status NOT IN ('TYPE II', 'ON-NET (Type II)', 'OFF-NET', 'OFF-NET (Restricted)', '13', '14', '15') OR crm_bldg.on_net_status IS NULL) AND
        (ST_Intersects(location, ST_Transform(ST_MakeEnvelope(' + map_extent_xyxy_min_max + ', ' +  map_crs_less_epsg + '), 4326))))