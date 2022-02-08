\timing
-- CTE which uses CASE statements to evaluate each Vector Sales attribute
with vector as (select
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
        cci_scrub.fiber_provider,
        cci_scrub.is_cci_power_available,
        cci_scrub.if_no_cci_power_is_meter_avail,
        cci_scrub.power_company,
        cci_scrub.available_telco_provider,
        cci_scrub.revshare,
        cci_scrub.revshare_split,
        cci_scrub.landlord_name,
        cci_scrub.primary_property_interest,
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
        cci_scrub.tower_only,
        cci_scrub.stealth_structure_type,
        cci_scrub.faa_study_number,
        cci_scrub.faa_approved_height,
        cci_scrub.structure_id,
        cci_scrub.ground_elevation,
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
        cci_scrub.fiber_on_site,
        cci_scrub.empty_shelter_avail,
        cci_scrub.is_there_ccishelter_on_site,
        cci_scrub.meter_account_owner,
        cci_scrub.meter_account_number,
        cci_scrub.meter_local_utility_number,
        cci_scrub.faa_lighting_requirement,
        cci_scrub.stealth_tower_ind,
        st.s_structure_type,
        st.s_hgt_with_appurt,
        st.s_site_flag,
        st.s_bu_type_code ,
        case when st.s_structure_type in ('GUYED', 'MONOPOLE', 'SELF SUPPORT') then 'GREEN'
            when st.s_structure_type in ('DAS NODE', 'ROOFTOP', 'SINGLE USE', 'STEALTH') then 'RED'
            when st.s_structure_type in ('COMPOUND', 'OTHER', 'UNCLASSIFIED') then 'YELLOW'
            else 'RED' end as struc_type_clr,

       case when cci_scrub.open_bird_ticket = 'Y' then 'RED'
            when cci_scrub.open_bird_ticket <> 'Y' then 'GREEN'
            end as bird_ticket_clr,

       case when cci_scrub.faa_study_number = '' or cci_scrub.faa_study_number is null then 'GREEN'
            else 'RED'
            end as faa_clr,

       case when cci_scrub.sublease_consent_type like 'No Consent%' then 'GREEN'
            when cci_scrub.sublease_consent_type = 'Consent - Intercompany' then 'GREEN'
            when cci_scrub.sublease_consent_type in (
                                                    'Consent - No Standard Specified (NTBUW)',
                                                    'Consent - Non-Standard Requirement',
                                                    'Consent - NTBUW',
                                                    'Notice') then 'YELLOW'
            when cci_scrub.sublease_consent_type = 'Consent - Sole Discretion' then 'RED'
            else 'GREEN' end as consent_clr,

       case when cci_scrub.current_tower_capacity is not null then 'GREEN'
            else 'RED' end as capacity_clr,

       case when cci_scrub.legal_issue_flag = 'No' or cci_scrub.legal_issue_flag is NULL then 'GREEN'
            when cci_scrub.legal_issue_flag = 'Yes' then 'RED' end as legal_clr,

       case when cci_scrub.zoning_collo_process_type in ('Admin', 'Concurrent', 'Full', 'Over Counter') then 'YELLOW'
            when cci_scrub.zoning_collo_process_type = 'No Zoning' then 'GREEN' else 'GREEN' end as zoning_clr,

       case when cci_scrub.bp_collo_permit_type in ('BP', 'BP + EP', 'Other', 'BP + Other', 'Electrical') then 'RED'
            when cci_scrub.bp_collo_permit_type = 'None' then 'GREEN'
            else 'RED' end as permit_clr,

       case when cci_scrub.site_status = 'Active' then 'GREEN'
            else 'RED' end as status_clr,

       case when cci_scrub.am_study_required = 'No' or cci_scrub.am_study_required is NULL then 'GREEN'
            when cci_scrub.am_study_required = 'Yes' then 'RED'
            end as am_study_clr,
       st.geom
    from
        odw.gis_dw_private.private_cci_sites_scrubbing_vw cci_scrub
    join
        odw.gis_dw_private.private_cci_sites_vw st on cci_scrub.bus_unit = st.s_bus_unit
    where
        cci_scrub.structure_id in ('A','B','C','D','E','F','G','C1','C2','C3','C4','C5','C6','C7','C8','C9','C10','C11','C12','C13','C14','C15','C16','C17','C18','C19','C20') and cci_scrub.bus_unit in ({id_list})
        st.s_bu_type_code in ('TW', 'RT')
    order by cci_scrub.bus_unit, cci_scrub.structure_id)

-- Select statement below evaluates results of the above CTE named 'vector'
-- Uses ALL to quickly identify any rows that are completely GREEN
-- Otherwise if RED is anywhere IN the delimited list of column values the row is RED
-- Finally if the previous two conditions are not met, the row is YELLOW

select
    *,
    case when 'GREEN' = all(array[struc_type_clr,bird_ticket_clr,faa_clr,consent_clr,capacity_clr,legal_clr,zoning_clr,permit_clr,status_clr,am_study_clr]) then 'GREEN'
         when 'RED' in (struc_type_clr,bird_ticket_clr,faa_clr,consent_clr,capacity_clr,legal_clr,zoning_clr,permit_clr,status_clr,am_study_clr) then 'RED' else 'YELLOW'
    end as vector_category

from vector