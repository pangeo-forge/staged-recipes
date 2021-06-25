from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, MergeDim
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

months = [f"{m:02d}" for m in (2, 3, 4, 8, 9, 10)]
concat_months = ConcatDim("time_counter", keys=months)


def gen_url(variable, time_counter):
    base = (
        "https://swiftbrowser.dkrz.de/public/dkrz_035d8f6ff058403bb42f8302e6badfbc/"
        "SWOT_intercomparison/"
    )
    return base + f"2012-{month}_{variable}_cubic.nc"


surf_ocean_vars = ["dflux", "ssh", "sss", "sst", "tx_sur", "ty_sur", "u_surf", "v_surf"]
int_ocean_vars = []

pattern_dict = {
                "surf_1h": {"vars": surf_ocean_vars},
                "int_1d": {"vars": int_ocean_vars},
}


def create_recipe(key, patterns=pattern_dict, url_func=gen_url, concat_dim=concat_months):
#     url_func.__defaults__ = patterns[key]["args"]
    merge_dim = MergeDim("variable", keys=patterns[key]["vars"])
    pattern = FilePattern(url_func, merge_dim, concat_dim)
    return XarrayZarrRecipe(pattern, target_chunks={"time": 15})


recipes = {list(pattern_dict)[i]: create_recipe(list(pattern_dict)[i]) for i in range(2)}
