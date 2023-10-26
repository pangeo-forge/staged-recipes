from apache_beam import Create
import pandas as pd

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.transforms import OpenURLWithFSSpec, OpenWithXarray, StoreToZarr

input_url_pattern = "s3://noaa-nclimgrid-daily-pds/v1-0-0/grids/{yyyy}/ncdd-{yyyymm}-grd-scaled.nc"
def format_function(time):
    return input_url_pattern.format(
        yyyy=time.strftime("%Y"), 
        yyyymm=time.strftime("%Y%m")
    )

dates = pd.date_range("1951-01-01", "2023-09-01", freq="MS")
concat_dim = ConcatDim(name="time", keys=dates, nitems_per_file=None)
pattern = FilePattern(format_function, concat_dim)

recipe = (
    Create(pattern.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray(file_type=pattern.file_type)
    | StoreToZarr(
        store_name="noaa-nclimgrid.zarr",
        combine_dims=pattern.combine_dim_keys,
        target_chunks={"time": 1, "lat": 596, "lon": 1385},
    )
)
