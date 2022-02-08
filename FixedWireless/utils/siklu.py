__all__ = ['SIKLU_FIELDS', 'DEFAULT_MODEL', 'link_budget_api']

import time
import json
import requests
from requests.exceptions import Timeout

SIKLU_FIELDS = [
    'antenna', 'capacity', 'd_km', 'link_margin', 'model', 'modulation', 'oxygen_attenuation_km',
    'rain_attenuation_km', 'rain_attenuation_total'
]

DEFAULT_MODEL = 'EtherHaul-8010F/FX'


def link_budget_api(calling_object, model_name, row_series):

    siklu_url = 'https://siklulinkbudgetapi2.herokuapp.com/api/v1/calculate/link_capacity'

    parameters = {
     "model_name": model_name,
     "lat_s": row_series.lat,
     "lon_s": row_series.long,
     "lat_d": calling_object.latitude,
     "lon_d": calling_object.longitude,
     "availability": 99.9,
     "capacity": calling_object.min_speed,
     "spare": 2,
     "pol": 'v'
    }

    attempts = 0
    request_rxd = False
    request = None

    while (attempts <= 5) and (request_rxd is False):

        try:

            request = requests.get(siklu_url, params=parameters, timeout=(5, 10))
            request_rxd = True
            attempts += 1

        except Timeout as TO:
            attempts += 1
            request = None
            calling_object.logger.error(
                f"Siklu link budget api call for record ID:{row_series.id} timeout {attempts} of 5"
            )
            time.sleep(3)

    if not request_rxd:
        raise ConnectionError('Siklu Budget API Request Timed Out')

    result = json.loads(request.content)
    if result:

        calling_object.siklu_api_responses = calling_object.siklu_api_responses.append(result, ignore_index=True)
        return
    else:
        calling_object.logger.warning(f'No siklu API response for ID: {row_series.id}')
