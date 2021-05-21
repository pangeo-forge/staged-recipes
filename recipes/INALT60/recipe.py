from pangeo_forge_recipes.patterns import (
    ConcatDim,
    FilePattern,
    MergeDim,
    pattern_from_file_sequence,
)
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

months = [f"{m:02d}" for m in (2, 3, 4, 8, 9, 10)]
concat_months = ConcatDim("time_counter", keys=months)


def gen_url(variable, time_counter, freq, dim):
    base = (
        "https://data.geomar.de/downloads/20.500.12085/"
        "0e95d316-f1ba-47e3-b667-fc800afafe22/data/"
    )
    return base + f"INALT60_{freq}_{dim}_{variable}_{time_counter}.nc"


surf_ocean_vars = ["u", "v", "hts"]

pattern_dict = {
    "surf_ocean_4h": {"vars": surf_ocean_vars, "args": ("4h", "surface")},
    "surf_ocean_5d": {"vars": surf_ocean_vars, "args": ("5d", "surface")},
    "surf_flux_1d": {"vars": ["flux", "taux", "tauy"], "args": ("1d", "surface")},
    "int_ocean_1d": {"vars": ["ts", "u", "v", "w"], "args": ("1d", "upper1000m")},
}


def create_recipe(key, patterns=pattern_dict, url_func=gen_url, concat_dim=concat_months):
    url_func.__defaults__ = patterns[key]["args"]
    merge_dim = MergeDim("variable", keys=patterns[key]["vars"])
    pattern = FilePattern(url_func, merge_dim, concat_dim)
    return XarrayZarrRecipe(pattern, target_chunks={"time_counter": 15})


recipes = {list(pattern_dict)[i]: create_recipe(list(pattern_dict)[i]) for i in range(4)}


def grid_recipe():
    url = (
        "https://data.geomar.de/downloads/20.500.12085/"
        "0e95d316-f1ba-47e3-b667-fc800afafe22/data/"
        "INALT60_mesh_mask.nc"
    )
    pattern = pattern_from_file_sequence([url], "time_counter", 1)
    return XarrayZarrRecipe(pattern)


recipes["grid"] = grid_recipe()
