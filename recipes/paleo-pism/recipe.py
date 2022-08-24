# We will concatanate over the ensemble number (as a first attempt)

# create identification numbers 
ids = list(range(6000,6256))

# use the ensemble numbers to make a ConcatDim
from pangeo_forge_recipes.patterns import ConcatDim
id_concat_dim = ConcatDim("id", ids, nitems_per_file=1)

# define a function for making the URL
def make_url(id):
    return f"zip://geo_data/pism1.0_paleo06_{id}/geometry_paleo_1ka.nc::https://download.pangaea.de/dataset/940149/files/paleo_ensemble_geo_2022.zip"

# make a FilePattern
from pangeo_forge_recipes.patterns import FilePattern
pattern = FilePattern(make_url, id_concat_dim)

import numpy as np

def add_id_as_dim_and_coord(ds, fname):
    id_temp = np.array([fname[31:35]])
    ds = ds.expand_dims("id").assign_coords(id=("id",id_temp))   
    return ds


# make a recipe using the ConcatDim and FilePattern objects we just made
from pangeo_forge_recipes.recipes import XarrayZarrRecipe
recipe = XarrayZarrRecipe(pattern, 
                          inputs_per_chunk=1,
                          process_input=add_id_as_dim_and_coord)    # one per chunk because each nc is ~210 MB
