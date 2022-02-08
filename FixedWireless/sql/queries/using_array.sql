select s_bus_unit, s_long_dec, s_lat_dec from gis_dw_private.private_cci_sites_vw
where
    s_bus_unit = any (
        array(
            select bus_unit
            from gis_dw_private.private_cci_sites_scrubbing_vw
            where fiber_provider like any('{"%FPL%","%CROWN CASTLE%","%LIGHTOWER%"}')));

-- array() can only be used when selecting a single column