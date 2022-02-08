with lit as (
                select distinct on (n.buildingid)
                    n.buildingid,
                    n.name,
                    n.clli,
                    n.street,
                    n.city,
                    n.state,
                    n.wkt_geometry,
                    case when (l.building_type is null or n.pop_tf = true) then n.building_type else l.building_type end as structure,
                    case when (l.building_type is null or n.pop_tf = true) then 'ospi_buildings' else 'private_lit' end as c_type,
                    l.on_net_status
                from ospi.ne_dw_buildings n
                left join gis_dw_private.private_lit_buildings_vw l on l.clli = n.name

                ),

            lit_join as (
                    select ndb.name, lit.clli from ospi.ne_dw_buildings ndb
                    left join gis_dw_private.private_lit_buildings_vw lit on ndb.name = lit.clli
            )

            select
                distinct
                lit.buildingid,
                lit.name,
                lit.clli,
                lit.street,
                lit.city,
                lit.state,
                lit.structure,
                lit.on_net_status,
                case when lit_join.clli is not null then 'True' else 'False' end as data_consistency,
                count(fcc.*) fcc_cnt,
                lit.c_type


            from lit


            left join gis_dw_private.private_fcc_active_license_activities_vw as fcc on ST_Intersects(St_Buffer(lit.wkt_geometry, 0.000278), fcc.geom)

            join lit_join on lit.name = lit_join.name

            group by
                lit.buildingid,
                lit.name,
                lit.clli,
                lit.street,
                lit.city,
                lit.state,
                lit.structure,
                lit.on_net_status,
                data_consistency,
                lit.c_type
