from itertools import product

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, MergeDim
from pangeo_forge_recipes.recipes import XarrayZarrRecipe


def gen_url(time, variable):
    base = "https://swift.dkrz.de/v1/dkrz_035d8f6ff058403bb42f8302e6badfbc/SWOT_intercomparison/"
    return base + f"2012-{time}_{variable}_cubic.nc"


surf_ocean_vars = ["dflux", "ssh", "sss", "sst", "tx_sur", "ty_sur", "u_surf", "v_surf"]
# int_ocean_vars = []

var_dict = {
    "surf": {"vars": surf_ocean_vars},
    # "int": {"vars": int_ocean_vars},
}


def create_recipe(datatype, season, vars=var_dict, url_func=gen_url):
    """
    """
    merge_dim = MergeDim("variable", keys=vars[datatype]["vars"])

    month_range = (2, 3, 4,) if season == "fma" else (8, 9, 10,)
    months = [f"{m:02d}" for m in month_range]
    concat_dim = ConcatDim("time", keys=months)

    pattern = FilePattern(url_func, concat_dim, merge_dim)
    return XarrayZarrRecipe(pattern, target_chunks={"time": 6})


datatypes = list(var_dict)
seasons = ("fma", "aso")
recipes = {
    f"FESOM/{datatype}/{season}": create_recipe(datatype=datatype, season=season)
    for datatype, season in product(datatypes, seasons)
}
