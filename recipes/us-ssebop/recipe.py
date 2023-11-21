from datetime import date

import apache_beam as beam
import pandas as pd

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.transforms import Indexed, OpenURLWithFSSpec, StoreToZarr, T

input_url_pattern = (
    'https://edcintl.cr.usgs.gov/downloads/sciweb1/shared/uswem/web/'
    'conus/eta/modis_eta/daily/downloads/'
    'det{yyyyjjj}.modisSSEBopETactual.zip'
)

start = date(2001, 1, 1)
end = date(2022, 10, 7)
dates = pd.date_range(start, end, freq='1D')


def make_url(time: pd.Timestamp) -> str:
    return input_url_pattern.format(yyyyjjj=time.strftime('%Y%j'))


pattern = FilePattern(make_url, ConcatDim(name='time', keys=dates, nitems_per_file=1))


class Preprocess(beam.PTransform):
    """Preprocessor transform."""

    @staticmethod
    def _preproc(item: Indexed[T]) -> Indexed[T]:
        import numpy as np
        import rioxarray

        index, f = item
        time_dim = index.find_concat_dim('time')
        time_index = index[time_dim].value
        time = dates[time_index]

        da = rioxarray.open_rasterio(f.open()).drop('band')
        da = da.rename({'x': 'lon', 'y': 'lat'})
        ds = da.to_dataset(name='aet')
        ds = ds.expand_dims(time=np.array([time]))

        return index, ds

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(self._preproc)


recipe = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec(max_concurrency=10, open_kwargs={'compression': 'zip'})
    | Preprocess()
    | StoreToZarr(
        store_name='us-ssebop.zarr',
        combine_dims=pattern.combine_dim_keys,
        target_chunks={'time': 1, 'lat': int(2834 / 2), 'lon': int(6612 / 6)},
    )
)
