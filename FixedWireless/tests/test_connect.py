import pytest
import os
from FixedWireless.postgis.connect import ODW
from FixedWireless.postgis.queries import build_lit_building_query  # , optimized_build_lit_building_query


@pytest.fixture
def odw():
    return ODW(environment='ODW_PROD', username='CC_GEO_PRIVATE')


@pytest.fixture
def conn_params(odw):
    return odw.connection_params


@pytest.fixture
def qgis_username():
    return f"qgis{os.getlogin()}"


@pytest.fixture
def lit_building_query():
    test_latitude = 40.7390831
    test_longitude = -73.9913778
    test_distance = 2
    test_limit = 25

    return build_lit_building_query(test_longitude, test_latitude, test_distance, test_limit)


@pytest.fixture
def non_spatial_query():
    return "select * from gis_dw_private.private_cci_sites_scrubbing_vw limit 25"


@pytest.fixture
def limit_five():

    return "select buildingid, name, clli from ospi.ne_dw_buildings limit 5"


class TestODW:
    def test__init(self, conn_params):
        assert conn_params.get('user') == 'cc_geo_private'

    def test_connection(self, odw):
        assert not odw.connection.closed

    def test_cursor(self, odw, limit_five):
        odw.cursor.execute(limit_five)
        rows_found = odw.cursor.fetchall()
        assert len(rows_found) == 5
        assert len(rows_found[0]) == 3

    def test_user(self, odw, qgis_username):
        assert odw._qgis_user == qgis_username

        assert odw.user.lower() == 'cc_geo_private'

        with pytest.raises(EnvironmentError):
            odw.user = 'ccfodwadmin'

    def test_server_side_cursor(self, odw, limit_five):
        sc = odw.serverSideCursor('test_cursor', refresh=True)
        sc.execute(limit_five)
        rows_found = sc.fetchall()
        assert len(rows_found) == 5

        with pytest.raises(ValueError):
            odw.serverSideCursor('bad_cursor_name')

    def test_create_engine(self, odw):
        eng = odw.engine
        con = eng.connect()
        assert eng is not None
        assert not con.closed

    def test_query_to_pandas_data_frame(self, odw, lit_building_query, non_spatial_query):

        df = odw.queryToDataFrame(non_spatial_query, flavor='pandas')
        assert int(df.shape[0]) == 25

    def test_query_to_geopandas_data_frame(self, odw, lit_building_query, non_spatial_query):

        gdf = odw.queryToDataFrame(lit_building_query, flavor='geopandas', geometry_column='pnt_geom')
        assert all([col_name in gdf.columns.values for col_name in ['candidate_type', 'id', 'cand_dist_km', 'long', 'lat', 'height', 'pnt_geom']])
        assert gdf.iloc[0]['candidate_type'] == 'ospi'

    def test_query_to_esri_data_frame(self, odw, lit_building_query, non_spatial_query):

        sedf = odw.queryToDataFrame(lit_building_query, flavor='esri', geometry_column='pnt_geom')

        assert int(sedf.shape[0]) == 25
        assert all([col_name in sedf.columns.values for col_name in ['candidate_type', 'id', 'cand_dist_km', 'long', 'lat', 'height', 'SHAPE']])
        assert sedf.iloc[0]['candidate_type'] == 'ospi'