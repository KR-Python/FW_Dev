import pytest
import pandas as pd
from numpy import arange
from time import sleep
from FixedWireless.utils.google import get_elevation_path, get_min_clearance


@pytest.fixture
def sample_candidates():
    # lat_one, long_one = 40.7390831, -73.9913778
    SRC_CAND = r'C:\Users\kryan\Documents\Local_Pro_Projects\Fixed Wireless\FixedWireless\scratch_folder\TestProject_Candidate_file.csv'
    candidates = pd.read_csv(SRC_CAND).sample(10, axis=0)

    yield candidates[['lat', 'long', 'height']].groupby((arange(len(candidates)) // 2))


def test_get_elevation_path(sample_candidates):
    results = []
    for idx, pair in sample_candidates:
        row1, row2 = pair.values
        lt1, lo1, hgt1 = row1.tolist()
        lt2, lo2, hgt2 = row2.tolist()

        result = get_elevation_path(lt1, lo1, lt2, lo2, hgt1)

        results.append(result)

    assert not any([x == 'Delete' for x in results])
