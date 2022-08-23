# We will concatanate over the ensemble number (as a first attempt)

# create ensemble numbers
ensemble_numbers = list(range(6000,6256))

# use the ensemble numbers to make a ConcatDim
from pangeo_forge_recipes.patterns import ConcatDim
enNum_concat_dim = ConcatDim("ensemble_number", ensemble_numbers, nitems_per_file=1)

# define a function for making the URL
def make_url(ensemble_number):
    return f"zip://geo_data/pism1.0_paleo06_{ensemble_number}/geometry_paleo_1ka.nc::https://download.pangaea.de/dataset/940149/files/paleo_ensemble_geo_2022.zip"

# make a FilePattern
from pangeo_forge_recipes.patterns import FilePattern
pattern = FilePattern(make_url, enNum_concat_dim)

# make a recipe using the ConcatDim and FilePattern objects we just made
from pangeo_forge_recipes.recipes import XarrayZarrRecipe
recipe = XarrayZarrRecipe(pattern, inputs_per_chunk=1)    # one per chunk because each nc is ~210 MB




