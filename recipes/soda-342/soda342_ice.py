import pandas as pd

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes import XarrayZarrRecipe


def make_full_path(time):
    y, m, d = time.year, time.month, time.day
    base = "https://dsrs.atmos.umd.edu/DATA/soda3.4.2/ORIGINAL/ice/"
    filename = f"soda3.4.2_5dy_ice_or_{y}_{m:02}_{d:02}.nc"
    return base + filename


dates = pd.date_range("1992-01-05", "2020-12-23", freq="5D")

concat_dim = ConcatDim("time", keys=dates, nitems_per_file=1)

pattern = FilePattern(make_full_path, concat_dim)

recipe = XarrayZarrRecipe(pattern)
