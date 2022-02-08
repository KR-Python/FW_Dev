select
       id,
       buildingid,
       name,
       clli,
       street,
       city,
       state,
       pop_tf,
       zip,
       workorder,
       security_level,
       address_notes,
       building_type,
       legacy_fid,
       legacy_owner,
       ST_IsValidReason(wkt_geometry) as geom_reason,
       ST_MakeValid(wkt_geometry) as geom


    from odw.ospi.ne_dw_buildings;

--location (centroid geometry) left out