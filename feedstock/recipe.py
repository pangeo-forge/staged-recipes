from dataclasses import dataclass

import apache_beam as beam
import pandas as pd
import s3fs
import xarray as xr
from beam_pyspark_runner.pyspark_runner import PySparkRunner
from pangeo_forge_ndpyramid.transforms import StoreToPyramid

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.storage import FSSpecTarget
from pangeo_forge_recipes.transforms import OpenURLWithFSSpec, OpenWithXarray

dates = pd.date_range('1981-09-01', '1981-10-01', freq='D')

URL_FORMAT = (
    'https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/'
    'v2.1/access/avhrr/{time:%Y%m}/oisst-avhrr-v02r01.{time:%Y%m%d}.nc'
)


def make_url(time):
    return URL_FORMAT.format(time=time)


time_concat_dim = ConcatDim('time', dates, nitems_per_file=1)
pattern = FilePattern(make_url, time_concat_dim)


#  NOTE: target uses the EMR serverless execution role (veda-data-reader-dev)
target_fsspec_kwargs = {'anon': False, 'client_kwargs': {'region_name': 'us-west-2'}}
fs_target = s3fs.S3FileSystem(**target_fsspec_kwargs)
target_root = FSSpecTarget(fs_target, 's3://veda-pforge-emr-outputs-v4')


@dataclass
class SelectSingleZlev(beam.PTransform):
    def select_single_zlev(self, ds: xr.Dataset) -> xr.Dataset:
        return ds.isel(zlev=0).drop('zlev')

    def expand(self, pcoll):
        return pcoll | 'Select single zlev' >> beam.MapTuple(
            lambda k, v: (k, self.select_single_zlev(v))
        )


with beam.Pipeline(runner=PySparkRunner()) as p:
    (
        p
        | beam.Create(pattern.items())
        | OpenURLWithFSSpec(fsspec_sync_patch=True)
        | OpenWithXarray(file_type=pattern.file_type)
        | SelectSingleZlev()
        | 'Write Pyramid Levels'
        >> StoreToPyramid(
            target_root=target_root,
            store_name='oisst_pyramid_2_lvl_3_week_pyramid_sync_a2.zarr',
            epsg_code='4326',
            rename_spatial_dims={'lon': 'longitude', 'lat': 'latitude'},
            levels=2,
            combine_dims=pattern.combine_dim_keys,
        )
    )
