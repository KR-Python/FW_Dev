__all__ = [
    'build_lit_building_query', 'build_asd_real_estate_query',
    'build_mgt_real_estate_query', 'build_macro_cci_sites_query',
    'build_lit_building_rank_query', 'build_macro_cci_sites_rank_query',
    'build_asd_real_estate_rank_query', 'build_mgt_real_estate_rank_query',
    'build_cc_scrub_query','optimized_build_lit_building_query', 'CANDIDATE_TYPES', 'CANDIDATE_FIELDS',
    'CCI_SITES_RANKING_FIELDS', 'LIT_BUILDING_RANKING_FIELDS',
    'REAL_ESTATE_ASD_RANKING_FIELDS','REAL_ESTATE_MGT_RANKING_FIELDS'
]

# CONSTANTS
CANDIDATE_TYPES = ['ospi', 'cci_sites', 're_asd', 're_mgt']

CANDIDATE_FIELDS = ['candidate_type', 'id', 'cand_dist_km', 'long', 'lat', 'height']

LIT_BUILDING_RANKING_FIELDS = [
    'id', 'name', 'clli', 'street', 'city', 'state', 'structure', 'status', 'data_consistent', 'score', 'fcc_count'
]

CCI_SITES_RANKING_FIELDS = [
    'id', 'crown_ae', 'structure', 'revshare', 'name', 'open_levels', 'current_tower_capacity',
    'last_sa_tia_code_revision', 'fiber_on_site', 'fiber_provider', 'is_cci_power_available',
    'if_no_cci_power_is_meter_avail', 'power_company', 'active_apps_count_on_site', 'pt_id', 'poptier',
    'street', 'city', 'site_county', 'state', 'score'
]

REAL_ESTATE_ASD_RANKING_FIELDS = ['id', 'uid2', 'name', 'street', 'city', 'state', 'structure', 'Fi_Dist', 'score']

REAL_ESTATE_MGT_RANKING_FIELDS = ['id', 'uid2', 'name', 'city', 'state', 'structure', 'Fi_Dist', 'score']


def build_lit_building_query(longitude, latitude, distance, limit):
    lit_query = f"""
    select
        'ospi' as candidate_type,
        buildingid::text as id,
        round(
            (st_distance(
                st_geomfromtext('POINT({longitude} {latitude})', 4326)::geography,
                location::geography)/1000)::numeric, 
            2
        ) as cand_dist_km,
        st_x(vertices.geom) as long,
        st_y(vertices.geom) as lat,
        30::double precision as height,
        vertices.geom as pnt_geom
    from
        ospi.ne_dw_buildings o
    -- the lateral join below returns the cardinal extremities (N,S,E,W) of each polygon as points (returns n * 4 rows)
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


def optimized_build_lit_building_query(longitude, latitude, distance, limit):
    lit_query = f"""
    select
        'ospi' as candidate_type,
        buildingid::text as id,
        round((st_distance(st_geomfromtext('POINT({longitude} {latitude})', 4326)::geography,location::geography)/1000)::numeric, 2) as cand_dist_km,
        st_x(vertices.geom) as long,
        st_y(vertices.geom) as lat,
        30::double precision as height,
        vertices.geom as pnt_geom
    from
        ospi.ne_dw_buildings o
    -- the lateral join below returns the cardinal extremities (N,S,E,W) of each polygon as points (returns n * 4 rows)
    join lateral
        (
            select
                x.name,
                (public.cc_cardinal_vertices_from_polygon(st_buffer(x.wkt_geometry,-0.000012))).geom

            from ospi.ne_dw_buildings x

            where st_intersects(location, o.wkt_geometry)

        ) as vertices on o.name = vertices.name

    where st_dwithin(st_geomfromtext('POINT({longitude} {latitude})', 4326)::geography, location::geography, ({distance}*1000)::double precision) 
    order by cand_dist_km limit {limit}
    """

    return lit_query


def build_asd_real_estate_query(longitude, latitude, distance, limit):
    re_asd_query = f"""
                select
                    're_asd' as candidate_type, 
                    mastersiteid as id, 
                    round((st_distance(st_geomfromtext('POINT({longitude} {latitude})', 4326)::geography, st_transform(geom, 4326)::geography)/1000)::numeric, 2) as cand_dist_km, 
                    londec as long, 
                    latdec as lat, 
                    height
                from gis_dw_private.private_site_alt_asd_vw
                where
                    st_distance(
                        st_geomfromtext('POINT({longitude} {latitude})', 4326)::geography, 
                        st_transform(geom, 4326)::geography
                        ) < ({distance} * 1000)
                order by cand_dist_km limit {limit}
                """

    return re_asd_query


def optimized_build_asd_real_estate_query(longitude, latitude, distance, limit):
    re_asd_query = f"""
                select
                    're_asd' as candidate_type, 
                    mastersiteid as id, 
                    round((st_distance(st_geomfromtext('POINT({longitude} {latitude})', 4326)::geography, st_transform(geom, 4326)::geography)/1000)::numeric, 2) as cand_dist_km, 
                    londec as long, 
                    latdec as lat, 
                    height
                from gis_dw_private.private_site_alt_asd_vw
                where
                    st_dwithin(st_geomfromtext('POINT({longitude} {latitude})', 4326)::geography, 
                        st_transform(geom, 4326)::geography, ({distance} * 1000)::double precision)
                order by cand_dist_km limit {limit}
                """

    return re_asd_query


def build_mgt_real_estate_query(longitude, latitude, distance, limit):
    re_mgt_query = f"""
                select
                    're_mgt' as candidate_type, 
                    esri_prinx as id, 
                    round((st_distance(st_geomfromtext('POINT({longitude} {latitude})', 4326)::geography, st_transform(geom, 4326)::geography)/1000)::numeric, 2) as cand_dist_km, 
                    lng_num as long, 
                    lat_num as lat, 
                    bld_hgt_num as height
                from gis_dw_private.private_site_alt_mgt_vw
                where
                    st_distance(
                        st_geomfromtext('POINT({longitude} {latitude})', 4326)::geography,
                        st_transform(geom, 4326)::geography
                        ) < ({distance} * 1000)
                order by cand_dist_km limit {limit}
                """
    return re_mgt_query


def build_macro_cci_sites_query(longitude, latitude, distance, limit, include_non_lit=False):
    if not include_non_lit:
        macro_query = f"""
                    select
                        'cci_sites' as candidate_type,
                        s_bus_unit as id,
                        round((st_distance(st_geomfromtext('POINT({longitude} {latitude})', 4326)::geography, geom::geography)/1000)::numeric, 2) as cand_dist_km, 
                        s_long_dec long, 
                        s_lat_dec lat, 
                        s_hgt_no_appurt height
                    from gis_dw_private.private_cci_sites_vw c
                    where 
                        s_bu_type_code in ('TW','RT') and 
                        s_external_flag = 1 and 
                        s_open_space is not null and 
                        s_bus_unit in (
                                        select 
                                            bus_unit
                                        from 
                                            gis_dw_private.private_cci_sites_scrubbing_vw
                                        where
                                            fiber_provider like '%FPL%'
                                            or fiber_provider like '%CROWN CASTLE%'
                                            or fiber_provider like '%LIGHTOWER%') and
                        st_distance(
                            st_geomfromtext('POINT({longitude} {latitude})', 4326)::geography,
                            geom::geography
                        ) < ({distance} * 1000)
                    order by cand_dist_km limit {limit}
                    """
    elif include_non_lit:

        macro_query = f"""
                    select
                        'cci_sites' as candidate_type,
                        s_bus_unit as id,
                        round((st_distance(st_geomfromtext('POINT({longitude} {latitude})', 4326)::geography, geom::geography)/1000)::numeric, 2) as cand_dist_km, 
                        s_long_dec long, 
                        s_lat_dec lat, 
                        s_hgt_no_appurt height
                    from gis_dw_private.private_cci_sites_vw c
                    where
                        s_bu_type_code in ('TW','RT') and 
                        s_external_flag = 1 and 
                        s_open_space is not null and 
                        st_distance(
                            st_geomfromtext('POINT({longitude} {latitude})', 4326)::geography,
                            geom::geography
                        ) < ({distance} * 1000)
                    order by cand_dist_km limit {limit}
            """

    # TODO make sure to add logic for: query_df['height'] = query_df['height'].fillna(float(30))
    return macro_query


# TODO projected coordinate system instead of using WGS84
def build_lit_building_rank_query(building_id_list):
    lit_rank_query = f"""
        with lit as (
                select distinct on (n.buildingid)
                    n.buildingid as id,
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
                where n.buildingid in {tuple(building_id_list)}
                
                ),
            
            lit_join as (
                    select ndb.name, lit.clli from ospi.ne_dw_buildings ndb 
                    left join gis_dw_private.private_lit_buildings_vw lit on ndb.name = lit.clli
            )
                
            select
                distinct
                lit.id,
                lit.name,
                lit.clli,
                lit.street,
                lit.city,
                lit.state,
                lit.structure,
                lit.on_net_status,
                case when lit_join.clli is not null then 'True' else 'False' end as data_consistency,
                count(fcc.*) fcc_cnt
            from lit
    
            
            left join gis_dw_private.private_fcc_active_license_activities_vw as fcc on ST_Intersects(St_Buffer(lit.wkt_geometry, 0.000278), fcc.geom)
            
            join lit_join on lit.name = lit_join.name
            
            group by
                lit.id,
                lit.name,
                lit.clli,
                lit.street,
                lit.city,
                lit.state,
                lit.structure,
                lit.on_net_status,
                data_consistency
                """
    return lit_rank_query


def build_macro_cci_sites_rank_query(cci_site_id_list):

    cci_rank_query = f"""
            select
                bus_unit as id,
                crown_ae,
                s_bu_type_code as structure,
                revshare,
                s_site_name as name,
                open_levels,
                current_tower_capacity,
                last_sa_tia_code_revision,
                fiber_on_site,
                fiber_provider,
                is_cci_power_available,
                if_no_cci_power_is_meter_avail,
                power_company,
                active_apps_count_on_site,
                s_pop_tier_id::int4 pt_id,
                s_pop_tier_name poptier,
                site_address as street,
                site_city as city,
                site_county,
                site_state as state,
                score::int4
            from gis_dw_private.private_cci_sites_scrubbing_vw as scrub_sites
            left join gis_dw_private.private_cci_sites_vw as raw_sites on bus_unit = s_bus_unit
            left join workspace.fw_building_rankings as c_ranks on s_bu_type_code = c_ranks.candidate_type
            where candidate_source = 'cci_sites' and scrub_sites.bus_unit in {str(tuple(cci_site_id_list)).replace(',)', ')')}
                group by
                bus_unit,
                crown_ae,
                s_bu_type_code,
                revshare,
                s_site_name,
                open_levels,
                current_tower_capacity,
                last_sa_tia_code_revision,
                fiber_on_site,
                fiber_provider,
                is_cci_power_available,
                if_no_cci_power_is_meter_avail,
                power_company,
                active_apps_count_on_site,
                s_pop_tier_id,
                s_pop_tier_name,
                site_address,
                site_city,
                site_county,
                site_state,
                score
                """

    return cci_rank_query


def build_asd_real_estate_rank_query(alt_real_estate_id_list):
    re_asd_rank_query = f"""
            select
                asd.mastersiteid as id,
                macrodealstatus as UID2,
                sourcesitename as name,
                streetaddress as street,
                city,
                state,
                structuretype as structure,
                fiberdistance as Fi_Dist,
                score::int4
            from
                gis_dw_private.private_site_alt_asd_vw asd
            left join workspace.fw_building_rankings as c_ranks on
                structuretype = c_ranks.candidate_type
            where
                candidate_source = 'cci_sites'
                and asd.mastersiteid in {str(tuple(alt_real_estate_id_list)).replace(',)', ')')}
            group by
                asd.mastersiteid,
                macrodealstatus,
                sourcesitename,
                streetaddress,
                city,
                state,
                structuretype,
                fiberdistance,
                score
            """

    return re_asd_rank_query


def build_mgt_real_estate_rank_query(mgt_real_estate_id_list):
    re_mgt_rank_query = f"""
            select
                mgt.esri_prinx as id,
                partner as UID2,
                alt_site_name as name,
                city,
                state,
                bu_type as structure,
                nearest_cci_any_fib_dist_m as Fi_Dist 
            from
                gis_dw_private.private_site_alt_mgt_vw mgt

            where

                mgt.esri_prinx in {str(tuple(mgt_real_estate_id_list)).replace(',)', ')')}

            """

    return re_mgt_rank_query


def build_cc_scrub_query(id_list):
    cc_scrub_query = f"""
            select
                bus_unit,
                crown_area ,
                crown_district,
                lats,
                longs,
                marketable,
                tower_height_w_appurt,
                portfolio ,
                structure_type,
                current_tower_capacity,
                open_levels,
                avail_ground_space_com,
                is_cci_power_available,
                zoning_jurisdiction,
                zoning_collo_process_type,
                zoning_collo_duration,
                permit_jurisdiction,
                bp_collo_permit_type,
                bp_collo_duration,
                consent_summary ,
                revshare,
                loa_required,
                loa_notes
            from
                gis_dw_private.private_cci_sites_scrubbing_vw
            where
                bus_unit in {str(tuple(id_list)).replace(',)', ')')}
            """
    return cc_scrub_query


def get_vector_sales_query(id_list):

    vector_query = f"""
    select
        cci_scrub.bus_unit,
        cci_scrub.crown_area,
        cci_scrub.crown_ae,
        cci_scrub.site_status,
        cci_scrub.consent_summary,
        cci_scrub.crown_district,
        cci_scrub.lats,
        cci_scrub.longs,
        cci_scrub.portfolio,
        cci_scrub.site_city,
        cci_scrub.site_county,
        cci_scrub.site_state,
        cci_scrub.site_zip,
        cci_scrub.marketable,
        cci_scrub.fiber_on_site,
        cci_scrub.fiber_provider,
        cci_scrub.is_cci_power_available,
        cci_scrub.if_no_cci_power_is_meter_avail,
        cci_scrub.power_company,
        cci_scrub.available_telco_provider,
        cci_scrub.revshare,
        cci_scrub.revshare_split,
        cci_scrub.landlord_name,
        cci_scrub.primary_property_interest,
        cci_scrub.tower_only,
        cci_scrub.fle,
        cci_scrub.leased_area_sq_ft,
        cci_scrub.compound_sq_ft,
        cci_scrub.compnd_expand,
        cci_scrub.avail_ground_space_com,
        cci_scrub.add_land_option,
        cci_scrub.option_sq_ft,
        cci_scrub.sublease_consent_type,
        cci_scrub.config_changes_consent_type,
        cci_scrub.loa_required,
        cci_scrub.zoning_jurisdiction,
        cci_scrub.zoning_collo_process_type,
        cci_scrub.zoning_ant_add_process_type,
        cci_scrub.zoning_ant_swap_process_type,
        cci_scrub.zoning_ground_process_type,
        cci_scrub.permit_jurisdiction,
        cci_scrub.bp_collo_permit_type,
        cci_scrub.bp_ant_add_permit_type,
        cci_scrub.bp_ant_swap_permit_type,
        cci_scrub.bp_ground_permit_type,
        cci_scrub.tower_lighted,
        cci_scrub.largest_length,
        cci_scrub.stealth_tower_ind,
        cci_scrub.stealth_structure_type,
        cci_scrub.faa_study_number,
        cci_scrub.faa_approved_height,
        cci_scrub.structure_id,
        cci_scrub.ground_elevation,
        cci_scrub.faa_lighting_requirement,
        cci_scrub.man_lift_req,
        cci_scrub.nearest_airport_distance,
        cci_scrub.site_on_si_tracker,
        cci_scrub.am_study_required,
        cci_scrub.abandoned_equipment_on_tower,
        cci_scrub.sa_on_file,
        cci_scrub.current_tower_capacity,
        cci_scrub.modification_expected,
        cci_scrub.can_it_be_ibm,
        cci_scrub.hist_migratory_bird_issues,
        cci_scrub.open_bird_ticket,
        cci_scrub.validated_fcc_registration,
        cci_scrub.legal_issue_flag,
        st.s_structure_type,
        st.s_hgt_with_appurt,
        st.s_site_flag,
        case when st.s_structure_type in ('GUYED', 'MONOPOLE', 'SELF SUPPORT') then 'GREEN'
            when st.s_structure_type in ('DAS NODE', 'ROOFTOP', 'SINGLE USE', 'STEALTH') then 'RED'
            when st.s_structure_type in ('COMPOUND', 'OTHER', 'UNCLASSIFIED') then 'YELLOW'
            else 'RED' end as STRUC_TYPE_CLR,

       case when cci_scrub.open_bird_ticket = 'Y' then 'RED'
            when cci_scrub.open_bird_ticket <> 'Y' then 'GREEN'
            end as BIRD_TICKET_CLR,

       case when cci_scrub.faa_study_number = '' or cci_scrub.faa_study_number is null then 'GREEN'
            when cci_scrub.faa_study_number is not null then 'RED'
            end as FAA_CLR,

       case when cci_scrub.sublease_consent_type like 'No Consent%' then 'GREEN'
            when cci_scrub.sublease_consent_type = 'Consent - Intercompany' then 'GREEN'
            when cci_scrub.sublease_consent_type in (
                'Consent - No Standard Specified (NTBUW)',
                'Consent - Non-Standard Requirement',
                'Consent - NTBUW',
                'Notice') then 'YELLOW'
            when cci_scrub.sublease_consent_type = 'Consent - Sole Discretion' then 'RED'
            else 'GREEN' end as CONSENT_CLR,

       case when cci_scrub.current_tower_capacity is not null then 'GREEN'
            else 'RED' end as CAPACITY_CLR,

       case when cci_scrub.legal_issue_flag = 'No' or cci_scrub.legal_issue_flag is NULL then 'GREEN'
            when cci_scrub.legal_issue_flag = 'Yes' then 'RED' end as LEGAL_CLR,

       case when cci_scrub.zoning_collo_process_type in ('Admin', 'Concurrent', 'Full', 'Over Counter') then 'YELLOW'
            when cci_scrub.zoning_collo_process_type = 'No Zoning' then 'GREEN' else 'GREEN' end as ZONING_CLR,

       case when cci_scrub.bp_collo_permit_type in ('BP', 'BP + EP', 'Other', 'BP + Other', 'Electrical') then 'RED'
            when cci_scrub.bp_collo_permit_type = 'None' then 'GREEN'
            else 'RED' end as PERMIT_CLR
    from
        odw.gis_dw_private.private_cci_sites_scrubbing_vw cci_scrub
        
    join
        odw.gis_dw_private.private_cci_sites_vw st on cci_scrub.bus_unit = st.s_bus_unit
        
    where
        cci_scrub.structure_id in ('A','B','C','D','E','F','G','C1','C2','C3','C4','C5','C6','C7','C8','C9','C10','C11','C12','C13','C14','C15','C16','C17','C18','C19','C20') --and
        cci_scrub.bus_unit in ({id_list})
        
    order by 
        cci_scrub.bus_unit, cci_scrub.structure_id
    """

    return vector_query

