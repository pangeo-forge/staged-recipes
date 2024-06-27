import base64
import json
import os
from dataclasses import dataclass

import apache_beam as beam
import pandas as pd
import requests
import s3fs
import xarray as xr
from beam_pyspark_runner.pyspark_runner import PySparkRunner
from pangeo_forge_ndpyramid.transforms import StoreToPyramid
from requests.auth import HTTPBasicAuth

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, FileType, pattern_from_file_sequence
from pangeo_forge_recipes.storage import FSSpecTarget
from pangeo_forge_recipes.transforms import OpenURLWithFSSpec, OpenWithXarray, StoreToZarr, ConsolidateMetadata

ED_USERNAME = os.environ['EARTHDATA_USERNAME']
ED_PASSWORD = os.environ['EARTHDATA_PASSWORD']
CREDENTIALS_API = 'https://data.gesdisc.earthdata.nasa.gov/s3credentials'
SHORT_NAME = 'GPM_3IMERGDF.07'
CONCAT_DIMS = ['time']
IDENTICAL_DIMS = ['lat', 'lon']


dates = [
    d.to_pydatetime().strftime('%Y/%m/3B-DAY.MS.MRG.3IMERG.%Y%m%d')
    for d in pd.date_range('2001-01-01', '2001-01-08', freq='D')
]
URL_FORMAT = (
    'https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/'
    'v2.1/access/avhrr/{time:%Y%m}/oisst-avhrr-v02r01.{time:%Y%m%d}.nc'
)

earthdata_protocol = 's3'
# earthdata_protocol = 'https'


def make_filename(time):
    if earthdata_protocol == 'https':
        # https://data.gesdisc.earthdata.nasa.gov/data/GPM_L3/GPM_3IMERGDF.07/2023/07/3B-DAY.MS.MRG.3IMERG.20230731-S000000-E235959.V07B.nc4
        base_url = f'https://data.gesdisc.earthdata.nasa.gov/data/GPM_L3/{SHORT_NAME}/'
    else:
        base_url = f's3://gesdisc-cumulus-prod-protected/GPM_L3/{SHORT_NAME}/'
    return f'{base_url}{time}-S000000-E235959.V07B.nc4'


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


fsspec_open_kwargs = earthdata_auth(ED_USERNAME, ED_PASSWORD)

concat_dim = ConcatDim('time', dates, nitems_per_file=1)
pattern = FilePattern(make_filename, concat_dim)


@dataclass
class DropVarCoord(beam.PTransform):
    """Drops non-viz variables & time_bnds."""

    def _dropvarcoord(self, ds: xr.Dataset) -> xr.Dataset:
        ds = ds.drop_vars('time_bnds')
        ds = ds[['precipitation']]
        return ds

    def expand(self, pcoll):
        return pcoll | 'Drop var coord' >> beam.MapTuple(lambda k, v: (k, self._dropvarcoord(v)))


@dataclass
class TransposeCoords(beam.PTransform):
    """Transform to transpose coordinates for pyramids"""

    def _transpose_coords(self, ds: xr.Dataset) -> xr.Dataset:
        return ds.transpose('time', 'lat', 'lon')

    def expand(self, pcoll):
        return pcoll | 'Transpose Coords' >> beam.MapTuple(
            lambda k, v: (k, self._transpose_coords(v))
        )


#  NOTE: target uses the EMR serverless execution role (veda-data-reader-dev)
target_fsspec_kwargs = {'anon': False, 'client_kwargs': {'region_name': 'us-west-2'}}
fs_target = s3fs.S3FileSystem(**target_fsspec_kwargs)
target_root = FSSpecTarget(fs_target, 's3://veda-pforge-emr-outputs-v4')
# target_root = FSSpecTarget(fs_target, 's3://carbonplan-scratch')

# from pangeo_forge_recipes.storage import CacheFSSpecTarget
# from pangeo_forge_recipes.transforms import CheckpointFileTransfer
# cache_target = CacheFSSpecTarget(s3fs.S3FileSystem(**target_fsspec_kwargs),   root_path="s3://carbonplan-scratch/pyramid/cache")


with beam.Pipeline(runner=PySparkRunner()) as p:
# with beam.Pipeline() as p:

    (
        p
        | beam.Create(pattern.items())
        # | CheckpointFileTransfer(transfer_target=cache_target,max_executors=10,concurrency_per_executor=10,fsspec_sync_patch=True)
        # | OpenURLWithFSSpec(open_kwargs=fsspec_open_kwargs, cache=None, fsspec_sync_patch=True)
        | OpenURLWithFSSpec(open_kwargs=fsspec_open_kwargs, fsspec_sync_patch=False)
        | OpenWithXarray(file_type=pattern.file_type)
        | DropVarCoord()
        | TransposeCoords()
        # | StoreToZarr(
        #     target_root=target_root,
        #     store_name='transpose_testing.zarr',
        #     combine_dims=pattern.combine_dim_keys,
        # )
        | StoreToPyramid(
        target_root=target_root,
        store_name='transpose_testing.zarr',
        epsg_code='4326',
        pyramid_kwargs={"x": "lon", "y": "lat"},
        pyramid_method = 'resample',
        levels=2,
        combine_dims=pattern.combine_dim_keys,
        )
        | ConsolidateMetadata()

    )

# s5cmd rm 's3://carbonplan-scratch/transpose_testing.zarr/*'

# Testing Zarr to Pyr



# pattern = pattern_from_file_sequence(
#     [
#         "s3://carbonplan-scratch/gpm_imerg_2_week_resample_a3.zarr"
#     ],
#     concat_dim="time",
# )


# pattern = pattern_from_file_sequence(
#     [
#         "s3://veda-pforge-emr-outputs-v4/gpm_imerg_s3_branch_stz_s3_1yr_sync.zarr"
#     ],
#     concat_dim="time",
# )

# with beam.Pipeline(runner=PySparkRunner()) as p:
# with beam.Pipeline() as p:

#     (
#         p
#         | beam.Create(pattern.items())    
#         | OpenWithXarray(file_type=FileType("zarr"), xarray_open_kwargs={"chunks": {}})
#         | StoreToPyramid(
#         target_root=target_root,
#         store_name='gpm_imerge_resample_3day_consolidate.zarr',
#         epsg_code="4326",
#         pyramid_method="resample",
#         pyramid_kwargs={"x": "lon", "y": "lat"},
#         levels=3,
#         combine_dims=pattern.combine_dim_keys,
#     )
#     | ConsolidateMetadata()
#     )

# s5cmd rm 's3://carbonplan-scratch/transpose_testing.zarr/*'
# Note: For testing, we're trying two levels. Ideally we should generate 4 levels
# import morecantile
# tms = morecantile.tms.get("WebMercatorQuad")
# tms.zoom_for_res(10000)
# 4
