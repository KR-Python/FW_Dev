SELECT *

    FROM gis_dw_private.private_cci_sites_vw

    WHERE s_bu_type_code IN ('TW', 'RT') AND
          s_external_flag = 1 AND
          s_open_space IS NOT NULL;