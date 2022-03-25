import datetime
import pandas as pd
import xarray as xr
from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes.xarray_zarr import XarrayZarrRecipe
import zarr

BASE_URL = 'http://noresg.nird.sigma2.no/thredds/fileServer/esg_dataroot/cmor/CMIP6/CMIP/NCC/NorESM2-LM/historical/r1i1p1f1/Omon/vmo/gr/v20190815/vmo_Omon_NorESM2-LM_historical_r1i1p1f1_gr_'

dates = pd.date_range("1850-01", "2010-01", freq="10YS")

time_concat_dim = ConcatDim("time", keys=dates)


def make_url(time):
    """With a start date as input, return a url terminating in
    ``{start}-{end}.nc`` where end is 10 years after the start
    date for years other than 2010. If the start date is 2010,
    the end date will be 5 years after the start date.
    
    :param date: The start date.
    """
    # assign 10 year interval for all years aside from 2010
    freq = "10YS" if time.year != 2010 else "5YS"

    # make a time range based on the assigned interval
    start, end = pd.date_range(time, periods=2, freq=freq)

    # subtract one day from the end of the range
    end = end - datetime.timedelta(days=1)

    # return the url with the timestamp in '%Y%m' format
    return f"{BASE_URL}{start.strftime('%Y%m')}-{end.strftime('%Y%m')}.nc"


pattern = FilePattern(make_url, time_concat_dim)

# Decide on chunk size by downloading local copy of 1 file
local_path = "vmo_Omon_NorESM2-LM_historical_r1i1p1f1_gr_185001-185912.nc"
ds = xr.open_dataset(local_path)

ntime = len(ds.time)       # the number of time slices
chunksize_optimal = 125e6  # desired chunk size in bytes
ncfile_size = ds.nbytes    # the netcdf file size
chunksize = max(int(ntime* chunksize_optimal/ ncfile_size),1)

target_chunks = ds.dims.mapping
target_chunks['time'] = chunksize

# the netcdf lists some of the coordinate variables as data variables. This is a fix which we want to apply to each chunk.
def set_bnds_as_coords(ds):
    new_coords_vars = [var for var in ds.data_vars if 'bnds' in var or 'bounds' in var]
    ds = ds.set_coords(new_coords_vars)
    return ds

recipe = XarrayZarrRecipe(
    pattern,
    target_chunks=target_chunks,
    process_chunk=set_bnds_as_coords,
    xarray_concat_kwargs={'join':'exact'},
)
