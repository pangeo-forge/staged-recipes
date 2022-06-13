from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

dates = [str(x) for x in range(1982,2018)]

def make_url(time):
    return "ftp://ftp.gfdl.noaa.gov/pub/William.Gregory/SICDA_forecasts_F02/"+time+".ice_daily.ens_mean.nc"

time_concat_dim = ConcatDim("time", dates)
pattern = FilePattern(make_url, time_concat_dim)

recipe = XarrayZarrRecipe(pattern, target_chunks={'time':180,'xT':10,'yT':10})
