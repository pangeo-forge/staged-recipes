import apache_beam as beam
import pandas as pd
import s3fs
from beam_pyspark_runner.pyspark_runner import PySparkRunner

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.storage import FSSpecTarget
from pangeo_forge_recipes.transforms import (
    ConsolidateMetadata,
    OpenURLWithFSSpec,
    OpenWithKerchunk,
    OpenWithXarray,
    StoreToZarr,
    WriteCombinedReference,
)

dates = pd.date_range('1981-09-01', '2000-09-01', freq='D')

URL_FORMAT = (
    'https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/'
    'v2.1/access/avhrr/{time:%Y%m}/oisst-avhrr-v02r01.{time:%Y%m%d}.nc'
)


def make_url(time):
    return URL_FORMAT.format(time=time)


time_concat_dim = ConcatDim('time', dates, nitems_per_file=1)
pattern = FilePattern(make_url, time_concat_dim)


# # NOTE: target uses the EMR serverless execution role (veda-data-reader-dev)
target_fsspec_kwargs = {'anon': False, 'client_kwargs': {'region_name': 'us-west-2'}}
fs_target = s3fs.S3FileSystem(**target_fsspec_kwargs)
target_root = FSSpecTarget(fs_target, 's3://veda-pforge-emr-outputs-v4')

with beam.Pipeline(runner=PySparkRunner()) as p:
    (
        p
        | beam.Create(pattern.items())
        | OpenURLWithFSSpec()
        | OpenWithKerchunk(file_type=pattern.file_type)
        | WriteCombinedReference(
            identical_dims=['lat', 'lon', 'zlev'],
            target_root=target_root,
            store_name='oisst_kerchunk_20_years',
            concat_dims=['time'],
            output_file_name='combined_oisst.parquet',
        )
    )
