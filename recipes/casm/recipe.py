import pandas as pd

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

dates = pd.date_range('2002-01-01', '2020-12-31', freq='Y')


def make_url(time):
    URL_FORMAT = 'https://zenodo.org/record/7072512/files/CASM_SM_{time:%Y}.nc'
    return URL_FORMAT.format(time=time)


time_concat_dim = ConcatDim('time', dates, nitems_per_file=1)
pattern = FilePattern(make_url, time_concat_dim)
recipe = XarrayZarrRecipe(pattern, inputs_per_chunk=1)
