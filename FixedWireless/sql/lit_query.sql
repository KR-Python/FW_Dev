SELECT *

    FROM gis_dw_private.private_cci_sites_scrubbing_vw

    WHERE fiber_provider LIKE '%FPL%' OR
          fiber_provider LIKE '%CROWN CASTLE%' OR
          fiber_provider LIKE '%LIGHTOWER%';