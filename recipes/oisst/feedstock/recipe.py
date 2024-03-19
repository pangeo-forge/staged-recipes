

# pangeo-forge-runner bake \
# --repo=~/carbonplan/pangeo_forge/staged-recipes/recipes/oisst \
# -f ~/Documents/carbonplan/pangeo_forge/staged-recipes/recipes/oisst/feedstock \
# --Bake.recipe_id=oisst \
# --Bake.job_name=local_test

import apache_beam as beam
import fsspec
import pandas as pd
import zarr
from apache_beam.runners.interactive.display import pipeline_graph
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.storage import FSSpecTarget
from pangeo_forge_recipes.transforms import (
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToPyramid,
)

dates = pd.date_range("1981-09-01", "1982-09-01", freq="D")

URL_FORMAT = (
    "https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/"
    "v2.1/access/avhrr/{time:%Y%m}/oisst-avhrr-v02r01.{time:%Y%m%d}.nc"
)


def make_url(time):
    return URL_FORMAT.format(time=time)


time_concat_dim = ConcatDim("time", dates, nitems_per_file=1)
pattern = FilePattern(make_url, time_concat_dim)


recipe = (
    beam.Create(pattern.items()) 
    | OpenURLWithFSSpec() 
    | OpenWithXarray(file_type=pattern.file_type)
    | 'Write Pyramid Levels' >> StoreToPyramid(
        store_name="pyramid",
        epsg_code="4326",
        rename_spatial_dims={"lon": "longitude", "lat": "latitude"},
        n_levels=4,
        pyramid_kwargs={"extra_dim": "zlev"},
        combine_dims=pattern.combine_dim_keys,
    ))



