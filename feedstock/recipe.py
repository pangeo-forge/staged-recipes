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

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.storage import FSSpecTarget
from pangeo_forge_recipes.transforms import OpenURLWithFSSpec, OpenWithXarray

ED_USERNAME = os.environ['EARTHDATA_USERNAME']
ED_PASSWORD = os.environ['EARTHDATA_PASSWORD']
CREDENTIALS_API = 'https://data.gesdisc.earthdata.nasa.gov/s3credentials'
SHORT_NAME = 'GPM_3IMERGDF.07'
CONCAT_DIMS = ['time']
IDENTICAL_DIMS = ['lat', 'lon']


dates = [
    d.to_pydatetime().strftime('%Y/%m/3B-DAY.MS.MRG.3IMERG.%Y%m%d')
    for d in pd.date_range('2000-06-01', '2000-06-08', freq='D')
]
URL_FORMAT = (
    'https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/'
    'v2.1/access/avhrr/{time:%Y%m}/oisst-avhrr-v02r01.{time:%Y%m%d}.nc'
)


def make_filename(time):
    base_url = f'https://data.gesdisc.earthdata.nasa.gov/data/GPM_L3/{SHORT_NAME}/'
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


def earthdata_auth(username: str, password: str):
    token = get_earthdata_token(username, password)
    return {'headers': {'Authorization': f'Bearer {token}'}}


fsspec_open_kwargs = earthdata_auth(ED_USERNAME, ED_PASSWORD)

concat_dim = ConcatDim('time', dates, nitems_per_file=1)
pattern = FilePattern(make_filename, concat_dim)


#  NOTE: target uses the EMR serverless execution role (veda-data-reader-dev)
target_fsspec_kwargs = {'anon': False, 'client_kwargs': {'region_name': 'us-west-2'}}
fs_target = s3fs.S3FileSystem(**target_fsspec_kwargs)
target_root = FSSpecTarget(fs_target, 's3://veda-pforge-emr-outputs-v4')
# target_root = FSSpecTarget(fs_target, 's3://carbonplan-scratch/pyresample')


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
        return ds.transpose("time", "lat", "lon")


    def expand(self, pcoll):
        return pcoll | 'Transpose Coords' >> beam.MapTuple(
            lambda k, v: (k, self._transpose_coords(v))
        )



with beam.Pipeline() as p:
    (
        p
        | beam.Create(pattern.items())
        | OpenURLWithFSSpec(open_kwargs=fsspec_open_kwargs, fsspec_sync_patch=True)
        | OpenWithXarray(file_type=pattern.file_type)
        | DropVarCoord()
        | TransposeCoords()
        | 'Write Pyramid Levels'
        >> StoreToPyramid(
            target_root=target_root,
            store_name='gpm_imerg_3_lvl_8day.zarr',
            epsg_code='4326',
            rename_spatial_dims={'lon': 'longitude', 'lat': 'latitude'},

            # pyramid_method = 'resample',
            levels=3,
            combine_dims=pattern.combine_dim_keys,
        )
    )


# s5cmd rm 's3://carbonplan-scratch/pyresample/gpm_imerg_2_lvl_3day.zarr/*'
# Note: For testing, we're trying two levels. Ideally we should generate 4 levels
# import morecantile
# tms = morecantile.tms.get("WebMercatorQuad")
# tms.zoom_for_res(10000)
# 4
