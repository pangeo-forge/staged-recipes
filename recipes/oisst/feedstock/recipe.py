import apache_beam as beam
import fsspec
import pandas as pd
import s3fs
from beam_pyspark_runner.pyspark_runner import PySparkRunner
from pangeo_forge_ndpyramid.transforms import StoreToPyramid

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.storage import FSSpecTarget
from pangeo_forge_recipes.transforms import ConsolidateMetadata, OpenURLWithFSSpec, OpenWithXarray

dates = pd.date_range('1981-09-01', '1981-09-03', freq='D')

URL_FORMAT = (
    'https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/'
    'v2.1/access/avhrr/{time:%Y%m}/oisst-avhrr-v02r01.{time:%Y%m%d}.nc'
)


def make_url(time):
    return URL_FORMAT.format(time=time)


time_concat_dim = ConcatDim('time', dates, nitems_per_file=1)
pattern = FilePattern(make_url, time_concat_dim)


### Creds

# NOTE: source uses the EMR serverless execution role (veda-data-reader-dev)
source_fsspec_kwargs = {
    'anon': False,
    'client_kwargs': {'region_name': 'us-west-2'},
}

# NOTE: target uses the EMR serverless execution role (veda-data-reader-dev)
target_fsspec_kwargs = {'anon': False, 'client_kwargs': {'region_name': 'us-west-2'}}
fs_target = s3fs.S3FileSystem(**target_fsspec_kwargs)
target_root = FSSpecTarget(fs_target, 's3://veda-pforge-emr-outputs-v4')


## NOAA-OISST is 1/4 spatial res. At the equator that's ~ 27770 meters, so we should have two pyramid levels
with beam.Pipeline(runner=PySparkRunner()) as p:
    (
        p
        | beam.Create(pattern.items())
        | OpenURLWithFSSpec(open_kwargs=source_fsspec_kwargs)
        | OpenWithXarray(file_type=pattern.file_type)
        | 'Write Pyramid Levels'
        >> StoreToPyramid(
            store_name='noaa-oisst-pyramid-4lvl.zarr',
            epsg_code='4326',
            rename_spatial_dims={'lon': 'longitude', 'lat': 'latitude'},
            levels=2,
            pyramid_kwargs={'extra_dim': 'zlev', 'clear_attrs': True},
            combine_dims=pattern.combine_dim_keys,
        )
        | ConsolidateMetadata()
    )
