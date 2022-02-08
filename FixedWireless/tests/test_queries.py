import pytest
from FixedWireless.postgis.queries import build_macro_cci_sites_rank_query, build_lit_building_query, optimized_build_lit_building_query, build_asd_real_estate_query, optimized_build_asd_real_estate_query, build_lit_building_rank_query
from FixedWireless.postgis.connect import ODW
from pandas.io.sql import read_sql



@pytest.fixture
def odw():

    return ODW(username='cc_geo_private')


@pytest.fixture
def bus_unit_numbers():

    return ['815871', '809402', '812196', '809154',
            '809153', '845297', '845298', '845299',
            '845300', '845301', '812663', '809470',
            '806559', '814187'
            ]


@pytest.fixture
def lit_building_query():
    test_latitude = 40.7390831
    test_longitude = -73.9913778
    test_distance = 2
    test_limit = 250

    return build_lit_building_query(test_longitude, test_latitude, test_distance, test_limit)


@pytest.fixture
def optimized_lit_building_query():
    test_latitude = 40.7390831
    test_longitude = -73.9913778
    test_distance = 2
    test_limit = 25

    return optimized_build_lit_building_query(test_longitude, test_latitude, test_distance, test_limit)


@pytest.fixture
def real_estate_asd_query():
    test_latitude = 40.7390831
    test_longitude = -73.9913778
    test_distance = 2
    test_limit = 25

    return build_asd_real_estate_query(test_longitude, test_latitude, test_distance, test_limit)


@pytest.fixture
def optimized_real_estate_asd_query():
    test_latitude = 40.7390831
    test_longitude = -73.9913778
    test_distance = 2
    test_limit = 25

    return optimized_build_asd_real_estate_query(test_longitude, test_latitude, test_distance, test_limit)


@pytest.fixture
def building_id_values(odw, lit_building_query):
    result = read_sql(lit_building_query, odw.connection)
    return result['id'].unique().tolist()


class TestQueries:

    def test_cci_ranks(self, odw, bus_unit_numbers):

        query = build_macro_cci_sites_rank_query(bus_unit_numbers)

        result = read_sql(query, odw.connection)

        assert int(result.shape[0]) > 1

    def test_lit_building_query(self, odw, lit_building_query):

        result = read_sql(lit_building_query, odw.connection)

        assert int(result.shape[0]) > 1

    def test_lit_building_rank_query(self, odw, building_id_values):

        query = build_lit_building_rank_query(building_id_values)

        result = read_sql(query, odw.connection)

        assert int(result.shape[0]) == len(building_id_values)

    def test_optimized_lit_building_query(self, odw, optimized_lit_building_query):

        result = read_sql(optimized_lit_building_query, odw.connection)

        assert int(result.shape[0]) > 1

    def test_real_estate_asd_query(self, odw, real_estate_asd_query):
        result = read_sql(real_estate_asd_query, odw.connection)

        assert int(result.shape[0]) > 1

    def test_optimized_real_estate_asd_query(self, odw, optimized_real_estate_asd_query):

        result = read_sql(optimized_real_estate_asd_query, odw.connection)

        assert int(result.shape[0]) > 1

