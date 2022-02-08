do
$$
begin
drop view if exists gis_dw_source.private_fixed_wireless_links_sor_vw;
drop view if exists gis_dw_private.private_fixed_wireless_links_vw;

create or replace view gis_dw_source.private_fixed_wireless_links_sor_vw as (
-- CTE to gather constituent point geometry values needed to construct link polylines
with fw_links as (
    select
       c.name,  -- circuit name contains both A & Z Location clli codes
       nullif(c.bandwidth, '')::int as bandwidth_mbps, -- Bandwidth will be used for symbology
       c.circuit_type,
       -- if a/z location clli is null then split circuit name value by hyphen delimiter to extract substring at position 3 or 4 (postgresql indices begin with 1)
       case
           when c.a_location_clli is NULL then split_part(c.name, '-', 3)
           else c.a_location_clli
       end as a_loc_clli,

       case
           when c.z_location_clli is NULL then split_part(c.name, '-', 4)
           else c.z_location_clli
       end as z_loc_clli,

           -- Extract X/Y Coordinate from location(point) geometry of OSPI buildings
       st_y(b1.location) a_latitude,
       st_x(b1.location) a_longitude,
       st_y(b2.location) z_latitude,
       st_x(b2.location) z_longitude

    from netcracker.nc_dw_circuits c
    -- for both joins we use a case statement to allow substituting the extracted substring for a/z location clli wherever they are NULL
    left join ospi.ne_dw_buildings b1 on case when (c.a_location_clli = b1.name or split_part(c.name, '-', 3) = b1.name) then 1 else 0 end = 1 -- Join 1 is for A-Location
    left join ospi.ne_dw_buildings b2 on case when (c.z_location_clli = b2.name or split_part(c.name, '-', 4) = b2.name) then 1 else 0 end = 1 -- Join 2 is for Z-Location
    where circuit_type = 'Fixed Wireless Link' -- Exclude any records that are not FW links
    )
-- Select from CTE & assemble polyline geometries
select
       fw_links.name,
       fw_links.bandwidth_mbps,
       fw_links.a_loc_clli,
       fw_links.z_loc_clli,
       fw_links.circuit_type,

       -- this case statement is to provide context when a link does not get created correctly
       case when (a_longitude is NULL or a_latitude is NULL) then 'Missing A-Location Lat/Long'
            when (z_longitude is NULL or z_latitude is NULL) then 'Missing Z-Location Lat/Long'
            else 'All Geometries Present'
       end as geom_info,

       case
           when not (
               a_longitude is NULL or
               a_latitude is NULL or
               z_longitude is NULL or
               z_latitude is NULL
               )
               then round(
                   st_length(
                       st_geomfromtext(
                            format('LINESTRING(%s %s, %s %s)', fw_links.a_longitude, fw_links.a_latitude, fw_links.z_longitude, fw_links.z_latitude), 4326)::geography)::numeric, 2)  -- WGS1984
       end as length_meters,

       -- format passes in x/y coordinate values to manually create a linestring/polyline from text whenever no A or Z coordinates are NULL
       case
           when not (
               a_longitude is NULL or
               a_latitude is NULL or
               z_longitude is NULL or
               z_latitude is NULL
               )
               then st_geomfromtext(
                        format('LINESTRING(%s %s, %s %s)', fw_links.a_longitude, fw_links.a_latitude, fw_links.z_longitude, fw_links.z_latitude), 4326)  -- WGS1984
       end as geom
from fw_links);

grant all on gis_dw_source.private_fixed_wireless_links_sor_vw to ccfodwadmin;
grant select on gis_dw_source.private_fixed_wireless_links_sor_vw to cc_geo_etl;

create or replace view gis_dw_private.private_fixed_wireless_links_vw as
    select * from gis_dw_source.private_fixed_wireless_links_sor_vw;


grant select on gis_dw_private.private_fixed_wireless_links_vw to cc_geo_private;
grant select on gis_dw_private.private_fixed_wireless_links_vw to cc_gis_services;
grant all on gis_dw_private.private_fixed_wireless_links_vw to ccfodwadmin;
grant select on gis_dw_private.private_fixed_wireless_links_vw to ccfodwreadonly;
grant select on gis_dw_private.private_fixed_wireless_links_vw to ccfodwreadwrite;
grant select on gis_dw_private.private_fixed_wireless_links_vw to cci_user;
grant all on gis_dw_private.private_fixed_wireless_links_vw to gis_admins;


exception when others then rollback;

commit;

end;
$$