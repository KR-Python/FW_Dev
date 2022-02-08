-- Division /3600/24 Query
with hrDiff1 as
    (select (extract(epoch from (mv.fle - now())/3600/24)) as diff
    from odw_dev.gis_dw_private.private_cci_sites_scrubbing_vw mv
    where mv.fle is not Null
        )
    select diff, case when diff < 1825 then 'YELLOW'
            when diff >= 1825 then 'GREEN'
            end as fle_clr

        from hrDiff1;

-- DatePart Query
with hrDiff2 as
    (select date_part('day', mv.fle - now()) as diff
    from odw_dev.gis_dw_private.private_cci_sites_scrubbing_vw mv
    where mv.fle is not Null
        )
    select diff, case when diff < 1825 then 'YELLOW'
            when diff >= 1825 then 'GREEN'
            end as fle_clr

        from hrDiff2;
