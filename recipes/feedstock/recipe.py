# AWS_PROFILE=devseed pangeo-forge-runner bake \
# --repo=~/Documents/carbonplan/pangeo_forge/staged-recipes/recipes/ \
# -f ~/Documents/carbonplan/pangeo_forge/staged-recipes/recipes/feedstock/config.py \
# --Bake.recipe_id=GPM_3IMERGDF.07 \
# --Bake.job_name=local_test


import base64
import json
import os

import apache_beam as beam
import pandas as pd
import requests
import xarray as xr
from requests.auth import HTTPBasicAuth

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.transforms import (
    Indexed,
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToPyramid,
)

ED_USERNAME = os.environ['EARTHDATA_USERNAME']
ED_PASSWORD = os.environ['EARTHDATA_PASSWORD']

earthdata_protocol = os.environ['PROTOCOL'] or 'https'
earthdata_protocol = 'https'
if earthdata_protocol not in ('https', 's3'):
    raise ValueError(f'Unknown ED_PROTOCOL: {earthdata_protocol}')

CREDENTIALS_API = 'https://data.gesdisc.earthdata.nasa.gov/s3credentials'
SHORT_NAME = 'GPM_3IMERGDF.07'
CONCAT_DIMS = ['time']
IDENTICAL_DIMS = ['lat', 'lon']

# 2023/07/3B-DAY.MS.MRG.3IMERG.20230731
dates = [
    d.to_pydatetime().strftime('%Y/%m/3B-DAY.MS.MRG.3IMERG.%Y%m%d')
    for d in pd.date_range('2000-06-01', '2014-01-01', freq='D')
]


def make_filename(time):
    if earthdata_protocol == 'https':
        # https://data.gesdisc.earthdata.nasa.gov/data/GPM_L3/GPM_3IMERGDF.07/2023/07/3B-DAY.MS.MRG.3IMERG.20230731-S000000-E235959.V07B.nc4
        base_url = f'https://data.gesdisc.earthdata.nasa.gov/data/GPM_L3/{SHORT_NAME}/'
    else:
        base_url = f's3://gesdisc-cumulus-prod-protected/GPM_L3/{SHORT_NAME}/'
    return f'{base_url}{time}-S000000-E235959.V07B.nc4'


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


class DropVarCoord(beam.PTransform):
    """Drops non-viz variables & time_bnds."""

    @staticmethod
    def _dropvarcoord(item: Indexed[xr.Dataset]) -> Indexed[xr.Dataset]:
        index, ds = item
        # Removing time_bnds since it doesn't have spatial dims
        ds = ds.drop_vars('time_bnds')
        return index, ds

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(self._dropvarcoord)


class TransposeCoords(beam.PTransform):
    """Transform to transpose coordinates for pyramids and drop time_bnds variable"""

    @staticmethod
    def _transpose_coords(item: Indexed[xr.Dataset]) -> Indexed[xr.Dataset]:
        index, ds = item
        ds = ds.transpose('time', 'lat', 'lon', 'nv')
        return index, ds

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(self._transpose_coords)


fsspec_open_kwargs = earthdata_auth(ED_USERNAME, ED_PASSWORD)


recipe = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec(open_kwargs=fsspec_open_kwargs)
    | OpenWithXarray(file_type=pattern.file_type)
    | TransposeCoords()
    | DropVarCoord()
    | 'Write Pyramid Levels'
    >> StoreToPyramid(
        store_name=SHORT_NAME,
        epsg_code='4326',
        rename_spatial_dims={'lon': 'longitude', 'lat': 'latitude'},
        n_levels=4,
        pyramid_kwargs={'extra_dim': 'nv'},
        combine_dims=pattern.combine_dim_keys,
    )
)


# ----------------------------------------
#### LOCAL RUNNING W/O pangeo-forge-runner
# ----------------------------------------

# import fsspec
# import zarr
# from pangeo_forge_recipes.storage import FSSpecTarget

# pipeline = beam.Pipeline()
# pipeline = beam.Pipeline(runner="DirectRunner", options=beam.pipeline.PipelineOptions(["--num_workers", '2', "--direct_running_mode", "multi_processing"]))


# fs = fsspec.get_filesystem_class("file")()
# path = 'tmp'
# target_root = FSSpecTarget(fs, path)

# fs = fsspec.get_filesystem_class("s3")()
# path = 's3://carbonplan-scratch/pyramid'
# target_root = FSSpecTarget(fs, path)

# import s3fs
# s3 = s3fs.S3FileSystem()
# s3.rm(path, recursive=True)

# pattern = pattern.prune()
# with pipeline as p:

#     (p | beam.Create(pattern.items())
#     | OpenURLWithFSSpec(open_kwargs=fsspec_open_kwargs)
#     | OpenWithXarray(file_type=pattern.file_type)
#     | TransposeCoords()
#     | DropVarCoord()

#     | 'Write Pyramid Levels'
#     >> StoreToPyramid(
#         target_root=target_root,
#         store_name=SHORT_NAME,
#         epsg_code='4326',
#         rename_spatial_dims={'lon': 'longitude', 'lat': 'latitude'},
#         n_levels=2,
#         pyramid_kwargs={'extra_dim': 'nv'},
#         combine_dims=pattern.combine_dim_keys,
#     ))
