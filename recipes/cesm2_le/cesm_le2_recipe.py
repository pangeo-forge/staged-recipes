import pandas as pd
import xarray as xr
from pangeo_forge_recipes.patterns import FilePattern
from pangeo_forge_recipes.patterns import ConcatDim
from pangeo_forge_recipes.recipes.xarray_zarr import XarrayZarrRecipe

df = pd.read_csv('/glade/work/mgrover/intake-esm-catalogs/cesm2-le.csv.gz')

df_sub = df[(df.component == 'atm') & 
            (df.frequency == 'month_1') & 
            (df.experiment == 'historical') & 
            (df.variable == 'T') &
            (df.experiment_number == 1161)
            ]

ds = xr.open_dataset(df_sub.path.values[0])

def determine_chunk_size(ds):
    ntime = len(ds.time)       # the number of time slices
    chunksize_optimal = 100e6  # desired chunk size in bytes
    ncfile_size = ds.nbytes    # the netcdf file size
    chunksize = max(int(ntime* chunksize_optimal/ ncfile_size),1)

    target_chunks = ds.dims.mapping
    target_chunks['time'] = chunksize 
    
    return target_chunks # a dictionary giving the chunk sizes in each dimension

ntime = len(ds.time)       # the number of time slices
chunksize_optimal = 100e6  # desired chunk size in bytes
ncfile_size = ds.nbytes    # the netcdf file size
chunksize = max(int(ntime* chunksize_optimal/ ncfile_size),1)

target_chunks = ds.dims.mapping
target_chunks['time'] = chunksize

# the netcdf lists some of the coordinate variables as data variables. This is a fix which we want to apply to each chunk.
def set_bnds_as_coords(ds):
    new_coords_vars = [var for var in ds.data_vars if 'bnds' in var or 'bounds' in var]
    ds = ds.set_coords(new_coords_vars)
    return ds

def make_full_path(time):
    '''
    Parameters
    ----------
    time : str
    
        A 13-character string, comprised of two 6-character dates delimited by a dash. 
        The first four characters of each date are the year, and the final two are the month.
        
        e.g. The time range from Jan 1850 through Dec 1949 is expressed as '185001-194912'.
            
    '''
    base_url = '/glade/campaign/cgd/cesm/CESM2-LE/timeseries/atm/proc/tseries/month_1/T/'
    return base_url + f'b.e21.BHISTcmip6.f09_g17.LE2-1161.009.cam.h0.T.{time}.nc'

keys = []
for path in df_sub.path.values:
    keys.append(str(path.split('.')[-2]))

time_concat_dim = ConcatDim("time", keys=keys)

pattern = FilePattern(make_full_path, time_concat_dim)

recipe = XarrayZarrRecipe(
    pattern, 
    target_chunks = target_chunks,
    #xarray_open_kwargs = {'decode_coords':False},
    xarray_concat_kwargs = {'coords': 'minimal', 'compat': 'override'},
)