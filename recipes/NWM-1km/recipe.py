import pandas as pd
from pangeo_forge_recipes.patterns import ConcatDim
from pangeo_forge_recipes.patterns import FilePattern
from pangeo_forge_recipes.recipes import XarrayZarrRecipe


dates = pd.date_range('1979-02-01 03:00', '2020-12-31 21:00', freq='3H')

time_concat_dim = ConcatDim("time", dates, nitems_per_file=1)

URL_FORMAT = ('s3://noaa-nwm-retrospective-2-1-pds/model_output/{time:%Y}/{time:%Y%m%d%H}00.LDASOUT_DOMAIN1.comp')

def make_url(time):
    return URL_FORMAT.format(time=time)

pattern = FilePattern(make_url, time_concat_dim)

target_chunks = {'time':72, 'x':512, 'y':512}
recipe = XarrayZarrRecipe(pattern, target_chunks=target_chunks)

run_function = recipe_pruned.to_function()

run_function()
