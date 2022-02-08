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
    st.s_structure_type,
    st.s_hgt_with_appurt,
    st.s_site_flag,
    case when st.s_structure_type in ('GUYED', 'MONOPOLE', 'SELF SUPPORT') then 'GREEN'
        when st.s_structure_type in ('DAS', 'ROOFTOP', 'SINGLE USE', 'STEALTH') then 'RED'
        when st.s_structure_type in ('COMPOUND', 'OTHER', 'UNCLASSIFIED') then 'YELLOW'
        else '' end as STRUC_TYPE_CLR,

    case when cci_scrub.stealth_tower_ind = 'Y' then 'RED'
        when cci_scrub.stealth_tower_ind <> 'Y' then 'GREEN'
        else '' end as STEALTH_TWR_IND_CLR,

   case when st.s_site_flag = '1' then 'GREEN'
        when st.s_site_flag <> '1' then 'RED'
        else '' end as SITE_FLAG_CLR,

   case when cci_scrub.current_tower_capacity = 'N/A' then ''
        when cci_scrub.current_tower_capacity < '200' then 'GREEN'
        when cci_scrub.current_tower_capacity >= '200' then 'RED'
        else '' end as CURRENT_TWR_CAPACITY_CLR,

   case when cci_scrub.faa_study_number = '' then 'GREEN'
        when cci_scrub.faa_study_number <> '' then 'RED'
        else '' end as FAA_STUDY_CLR,

   case when cci_scrub.open_bird_ticket = 'Y' then 'RED'
        when cci_scrub.open_bird_ticket <> 'Y' then 'GREEN'
        else '' end as OPEN_BIRD_TICKET_CLR,


   case when cci_scrub.fle is null then ''
        when 0 < date_part('day', cci_scrub.fle - now()) and date_part('day', cci_scrub.fle - now()) < 1825 then 'YELLOW'
        when date_part('day', cci_scrub.fle - now()) >= 1825 then 'GREEN'
        end as FLE_CLR,

   case when cci_scrub.primary_property_interest = 'MANAGED' then 'YELLOW'
        when cci_scrub.primary_property_interest = 'LEASED' then 'GREEN'
        else '' end as PROPERTY_INTEREST_CLR,

   case when cci_scrub.zoning_collo_process_type in ('Admin', 'Concurrent', 'Full', 'Over Counter') then 'YELLOW'
        when cci_scrub.zoning_collo_process_type = 'No Zoning' then 'GREEN'
        else '' end as ZONING_COLLO_PROCESS_TYPE_CLR,

   case when cci_scrub.zoning_ant_add_process_type in ('Admin', 'Concurrent', 'Full', 'Over Counter') then 'YELLOW'
        when cci_scrub.zoning_ant_add_process_type = 'No Zoning' then 'GREEN'
        else '' end as ZONING_ANT_ADD_PROCESS_TYPE_CLR,

   case when cci_scrub.zoning_ant_swap_process_type in ('Admin', 'Concurrent', 'Full', 'Over Counter') then 'YELLOW'
        when cci_scrub.zoning_ant_swap_process_type = 'No Zoning' then 'GREEN'
        else '' end as ZONING_ANT_SWAP_PROCESS_TYPE_CLR,

   case when cci_scrub.bp_collo_permit_type in ('BP', 'BP + EP', 'Other', 'BP + Other', 'Electrical') then 'YELLOW'
        when cci_scrub.bp_collo_permit_type = 'None' then 'GREEN'
        else '' end as BP_COLLO_PERMIT_TYPE_CLR,

   case when cci_scrub.bp_ant_add_permit_type in ('BP', 'BP + EP', 'Other', 'BP + Other', 'Electrical') then 'YELLOW'
        when cci_scrub.bp_ant_add_permit_type = 'None' then 'GREEN'
        else '' end as BP_ANT_ADD_PERMIT_TYPE_CLR,

   case when cci_scrub.bp_collo_permit_type in ('BP', 'BP + EP', 'Other', 'BP + Other', 'Electrical') then 'YELLOW'
        when cci_scrub.bp_collo_permit_type = 'None' then 'GREEN'
        else '' end as BP_ANT_SWAP_PERMIT_TYPE_CLR,

    -- config_changes_consent_type (sublease = first time install, config_changes = amendment)
   case when cci_scrub.sublease_consent_type in (
                                                'Consent - Intercompany',
                                                'No Consent or Notice Required',
                                                'No Consent or Notice Required - IC Blanket Agreement',
                                                'No Consent or Notice Required - Silent') then 'GREEN'
        when cci_scrub.sublease_consent_type in (
                                                'Consent - No Standard Specified (NTBUW)',
                                                'Consent - Non-Standard Requirement',
                                                'Consent - NTBUW',
                                                'Notice') then 'YELLOW'
        when cci_scrub.sublease_consent_type = 'Consent - Sole Discretion' then 'RED'

        else '' end as CONSENT_SUMMARY_CLR


from
    odw.gis_dw_private.private_cci_sites_scrubbing_vw cci_scrub
join
    odw.gis_dw_private.private_cci_sites_vw st on cci_scrub.bus_unit = st.s_bus_unit
where
    cci_scrub.structure_id in ('A','B','C','D','E','F','G','C1','C2','C3','C4','C5','C6','C7','C8','C9','C10','C11','C12','C13','C14','C15','C16','C17','C18','C19','C20')
order by cci_scrub.bus_unit, cci_scrub.structure_id

