from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes import XarrayZarrRecipe, setup_logging
import xarray as xr

dates = [str(x) for x in range(1982,2018)]

def make_url(time):
    return "ftp://ftp.gfdl.noaa.gov/pub/William.Gregory/SIC_observations_NT/nt_"+time+"_v01_n_neareststod_spear.nc"

time_concat_dim = ConcatDim("time", dates, nitems_per_file=365)
pattern = FilePattern(make_url, time_concat_dim, file_type='netcdf3')

recipe = XarrayZarrRecipe(pattern, inputs_per_chunk=2)
