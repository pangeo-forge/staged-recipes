from itertools import product

from pangeo_forge_recipes.patterns import (
    ConcatDim,
    FilePattern,
    MergeDim,
    pattern_from_file_sequence,
)
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

regions = ["GulfStream", "MidAtl", "MedSea", "Agulhas"]
regid_dict = {"GulfStream": 1, "MidAtl": 2, "MedSea": 3, "Agulhas": 4}
varS = ["gridU", "gridV", "gridT"]
varI = ["gridU", "gridV", "gridW", "gridT"]
varG = ["bathymetry", "meshmask"]
season_months = {"fma": "20140201-20140430", "aso": "20130801-20131031"}
url_base = "ftp://ftp.mercator-ocean.fr/download/users/cbricaud/ORCA36-T404"
depths = ["surface_hourly", "interior_daily"]


def make_recipe(region, season, depth):

    time_counter = season_months[season]
    vars = varS if depth == "surface_hourly" else varI
    step = "h" if depth == "surface_hourly" else "d"
    target_chunks = {"time_counter": 72} if depth == "surface_hourly" else {"time_counter": 2}
    subset_inputs = {"time_counter": 4} if depth == "surface_hourly" else {"time_counter": 8}

    def make_full_path(time_counter, variable):
        return url_base + f"/ORCA36-T404_1{step}AV_{time_counter}_{variable}_{region}.nc"

    concat_dim = ConcatDim("time_counter", keys=[time_counter])
    merge_dim = MergeDim("variable", keys=vars)
    file_pattern = FilePattern(make_full_path, concat_dim, merge_dim)

    recipe = XarrayZarrRecipe(
        file_pattern, target_chunks=target_chunks, subset_inputs=subset_inputs
    )

    return recipe


def make_recipe_grid(region, var):
    input_url_pattern = url_base + "/ORCA36-T404_{var}_{reg}.nc"
    input_urls = [input_url_pattern.format(reg=region, var=var)]
    file_pattern = pattern_from_file_sequence(input_urls, "mock_concat_dim", nitems_per_file=1)

    xarray_open_kwargs = {"engine": "scipy"} if var == "bathymetry" else {}
    recipe = XarrayZarrRecipe(file_pattern, xarray_open_kwargs=xarray_open_kwargs)
    return recipe


recipes = {
    f"Region{regid_dict[r]:02}/{d}/{s}": make_recipe(r, s, d)
    for r, s, d in product(regions, season_months.keys(), depths)
}

grids = {
    f"Region{regid_dict[r]:02}/grid/{v}": make_recipe_grid(r, v) for r, v in product(regions, varG)
}

recipes.update(grids)
