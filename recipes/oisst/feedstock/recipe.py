import apache_beam as beam
import pandas as pd
import fsspec 
from beam_pyspark_runner.pyspark_runner import PySparkRunner

from pangeo_forge_ndpyramid.transforms import StoreToPyramid

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.storage import FSSpecTarget
from pangeo_forge_recipes.transforms import OpenURLWithFSSpec, OpenWithXarray

dates = pd.date_range('1981-09-01', '1981-09-03', freq='D')

URL_FORMAT = (
    'https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/'
    'v2.1/access/avhrr/{time:%Y%m}/oisst-avhrr-v02r01.{time:%Y%m%d}.nc'
)


def make_url(time):
    return URL_FORMAT.format(time=time)


time_concat_dim = ConcatDim('time', dates, nitems_per_file=1)
pattern = FilePattern(make_url, time_concat_dim)


fs = fsspec.get_filesystem_class('s3')()
path = 's3://carbonplan-scratch/oisst_pyr/'
target_root = FSSpecTarget(fs, path)

with beam.Pipeline(runner=PySparkRunner()) as p:
    (
    p | 
    beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray(file_type=pattern.file_type)
    | 'Write Pyramid Levels'
    >> StoreToPyramid(
        target_root=target_root,
        store_name='pyramid',
        epsg_code='4326',
        rename_spatial_dims={'lon': 'longitude', 'lat': 'latitude'},
        levels=4,
        pyramid_kwargs={'extra_dim': 'zlev', 'clear_attrs': True},
        combine_dims=pattern.combine_dim_keys,
    )
    )


# s5cmd rm 's3://carbonplan-scratch/oisst_pyr/*'