import requests

__all__ = ['get_min_clearance', 'get_elevation_path', 'param_df_template']

API_KEY = ''


def get_elevation_path(lat1, lon1, lat2, lon2, height, samples="200", sensor="false", debug=False):
    # Key is currently set to test - use gapi_key once enabled

    url = f'https://maps.google.com/maps/api/elevation/json?path={str(lat1)},{str(lon1)}|{str(lat2)},{str(lon2)}&samples={samples}&sensor={sensor}&key={API_KEY}'
    response = requests.get(url)
    # Results will be in JSON format - convert to dict using requests functionality
    response = response.json()
    elevation_array = [resultset['elevation'] for resultset in response['results']]
    if debug:
        return elevation_array
    min_clearance = get_min_clearance(elevation_array, mast_height=height)

    if min_clearance < -300:
        return 'Delete'
    elif min_clearance < 0:
        return 'Fail'
    else:
        return 'Pass'


def get_min_clearance(elevation_path, mast_height):
    n = len(elevation_path)
    y0 = elevation_path[0] + mast_height
    y1 = elevation_path[-1] + mast_height
    m = (y1 - y0) / (n-1)
    link_path = [(m*x + y0) for x in range(n)]
    clearance = [x[0] - x[1] for x in zip(link_path, elevation_path)]
    return min(clearance)


def param_df_template(df):

    temp = dict()

    temp['Site Name'] = df['Project Name']
    temp['Latitude'] = df['Latitude']
    temp['Longitude'] = df['Longitude']
    temp['Site Enabled'] = 'False'
    temp['Device Name'] = df['Project Name']
    temp['Max Power (dBm)'] = '40'
    temp['Misc Loss (dB)'] = '0'
    temp['Antenna Type'] = 'EBAND_ISO'
    temp['Mechanical Azimuth (deg)'] = 0
    temp['Mechanical Downtilt (deg)'] = 0
    temp['Electrical Downtilt (deg)'] = 0
    temp['Antenna Height AGL (m)'] = 0
    temp['Beamforming Gain (dB)'] = 0
    temp['LTE PDSCH Offset (dB)'] = 0
    temp['Cell Load (%)'] = 0

    return temp
