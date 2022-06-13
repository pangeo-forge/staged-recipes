from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

dates = [str(x) for x in range(1982,2018)]

def make_url(record):
    return "ftp://ftp.gfdl.noaa.gov/pub/William.Gregory/SICDA_priors_F02/"+record+".input_mean.nc"

time_concat_dim = ConcatDim("record", dates)
pattern = FilePattern(make_url, time_concat_dim, file_type='netcdf3')

recipe = XarrayZarrRecipe(pattern, target_chunks={'record':97})
