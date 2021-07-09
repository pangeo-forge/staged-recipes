from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes.xarray_zarr import XarrayZarrRecipe


# This seems cumbersome and not elegant. 
# Is there a way to wrap the values of `url_dict` into functions programmatically?
def gfdl_esm4_url():
    return "ftp://ftp.gfdl.noaa.gov/perm/Alistair.Adcroft/MOM6-testing/OM4_05/ocean_hgrid.nc"


url_dict = {"GFDL-ESM4": gfdl_esm4_url}


# Preprocessors

# At the moment only a dummy func
def gfdl_pp_func(ds):
    ds.attrs["just_a_dummy_entru"] = "again just a dummy"
    return ds


preprocess_dict = {"GFDL-ESM4": gfdl_pp_func}


time_concat_dim = ConcatDim("dummy", list(range(0)))


def recipe_maker(grid_key):
    pp = preprocess_dict[grid_key]
    url = url_dict[grid_key]
    filepattern = FilePattern(url, time_concat_dim)
    return XarrayZarrRecipe(filepattern, target_chunks={"dummy": 1}, process_intput=pp)


recipes = {k: recipe_maker(k) for k in url_dict.keys()}
