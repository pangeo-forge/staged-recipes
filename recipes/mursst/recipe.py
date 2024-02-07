import base64
import json
import os
from dataclasses import dataclass, field
from typing import Dict, Set

import apache_beam as beam
import pandas as pd
import requests
import xarray as xr
import zarr
from requests.auth import HTTPBasicAuth

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.transforms import OpenWithKerchunk, WriteCombinedReference

HTTP_REL = 'http://esipfed.org/ns/fedsearch/1.1/data#'
S3_REL = 'http://esipfed.org/ns/fedsearch/1.1/s3#'

ED_USERNAME = os.environ['EARTHDATA_USERNAME']
ED_PASSWORD = os.environ['EARTHDATA_PASSWORD']
earthdata_protocol = os.environ['PROTOCOL'] or 'https'

if earthdata_protocol not in ('https', 's3'):
    raise ValueError(f'Unknown ED_PROTOCOL: {earthdata_protocol}')

CREDENTIALS_API = 'https://archive.podaac.earthdata.nasa.gov/s3credentials'
SHORT_NAME = 'MUR-JPL-L4-GLOB-v4.1'
CONCAT_DIMS = ['time']
IDENTICAL_DIMS = ['lat', 'lon']
SELECTED_VARS = ['analysed_sst', 'analysis_error', 'mask', 'sea_ice_fraction']

missing_date_strings = ['2021-02-20', '2021-02-21', '2022-11-09']
missing_dates = pd.to_datetime(missing_date_strings)
dates = [
    d.to_pydatetime().strftime('%Y%m%d')
    for d in pd.date_range('2002-06-01', '2002-08-31', freq='D')
    if d not in missing_dates
]


def make_filename(time):
    if earthdata_protocol == 'https':
        base_url = (
            f'https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/{SHORT_NAME}/'
        )
    else:
        base_url = f's3://podaac-ops-cumulus-protected/{SHORT_NAME}/'
    # example file: "/20020601090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc"
    return f'{base_url}{time}090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc'


concat_dim = ConcatDim('time', dates, nitems_per_file=1)
pattern = FilePattern(make_filename, concat_dim)


def get_earthdata_token(username, password):
    # URL for the Earthdata login endpoint
    login_url = 'https://urs.earthdata.nasa.gov/api/users/token'
    auth = HTTPBasicAuth(username, password)

    # Request a new token
    response = requests.get(f'{login_url}s', auth=auth)

    # Check if the request was successful
    if response.status_code == 200:
        if len(response.json()) == 0:
            # create new token
            response = requests.post(login_url, auth=auth)
            if response.status_code == 200:
                token = response.json()['access_token']
            else:
                raise Exception('Error: Unable to generate Earthdata token.')
        else:
            # Token is usually in the response's JSON data
            token = response.json()[0]['access_token']
        return token
    else:
        raise Exception('Error: Unable to retrieve Earthdata token.')


def get_s3_creds(username, password, credentials_api=CREDENTIALS_API):
    login_resp = requests.get(CREDENTIALS_API, allow_redirects=False)
    login_resp.raise_for_status()

    encoded_auth = base64.b64encode(f'{username}:{password}'.encode('ascii'))
    auth_redirect = requests.post(
        login_resp.headers['location'],
        data={'credentials': encoded_auth},
        headers={'Origin': credentials_api},
        allow_redirects=False,
    )
    auth_redirect.raise_for_status()

    final = requests.get(auth_redirect.headers['location'], allow_redirects=False)

    results = requests.get(CREDENTIALS_API, cookies={'accessToken': final.cookies['accessToken']})
    results.raise_for_status()

    creds = json.loads(results.content)
    return {
        'key': creds['accessKeyId'],
        'secret': creds['secretAccessKey'],
        'token': creds['sessionToken'],
        'anon': False,
    }


def earthdata_auth(username: str, password: str):
    if earthdata_protocol == 's3':
        return get_s3_creds(username, password)
    else:
        token = get_earthdata_token(username, password)
        return {'headers': {'Authorization': f'Bearer {token}'}}

@dataclass
class FilterVars(beam.PTransform):
    """Filter kerchunk variables by name."""

    keep: Set[str] = field(default_factory=set)

    @staticmethod
    def _filter(item: list[dict], keep: Set[str]) -> list[dict]:
        filtered = []
        for translated in item:
            # see Kerchunk docs for dropping a variable example
            translated['refs'] = {
                k: v
                for k, v in translated['refs'].items()
                if '/' not in k or k.split('/')[0] in keep
            }
            filtered.append(translated)
        return filtered

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(self._filter, keep=self.keep)


@dataclass
class ValidateDatasetDimensions(beam.PTransform):
    """Open the reference.json in xarray and validate dimensions."""

    expected_dims: Dict = field(default_factory=dict)

    @staticmethod
    def _validate(zarr_store: zarr.storage.FSStore, expected_dims: Dict) -> None:
        ds = xr.open_dataset(zarr_store, engine='zarr')
        if set(ds.dims) != expected_dims.keys():
            raise ValueError(f'Expected dimensions {expected_dims.keys()}, got {ds.dims}')
        for dim, bounds in expected_dims.items():
            if bounds is None:
                continue
            lo, hi = bounds
            actual_lo, actual_hi = round(ds[dim].data.min()), round(ds[dim].data.max())
            if actual_lo != lo or actual_hi != hi:
                raise ValueError(f'Expected {dim} range [{lo}, {hi}], got {actual_lo, actual_hi}')
        return ds

    def expand(
        self,
        pcoll: beam.PCollection,
    ) -> beam.PCollection:
        return pcoll | beam.Map(self._validate, expected_dims=self.expected_dims)


auth_args = earthdata_auth(ED_USERNAME, ED_PASSWORD)
    
recipe = (
    beam.Create(pattern.items())
    | OpenWithKerchunk(
        remote_protocol=earthdata_protocol,
        file_type=pattern.file_type,
        # lat/lon are around 5k, this is the best option for forcing kerchunk to inline them
        inline_threshold=6000,
        storage_options=auth_args,
    )
    | WriteCombinedReference(
        concat_dims=CONCAT_DIMS,
        identical_dims=IDENTICAL_DIMS,
        store_name=SHORT_NAME,
        # for running without a runner, use this target_root
        # target_root=fs_target,
        # mzz_kwargs={'coo_map': {"time": "cf:time"}, 'inline_threshold': 0}
    )
    #| ValidateDatasetDimensions(expected_dims={'time': None, 'lat': (-90, 90), 'lon': (-180, 180)})
)
