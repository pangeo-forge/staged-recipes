from itertools import product

from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

regions = ["GulfStream", "MidAtl", "MedSea", "Agulhas"]
regid_dict = {"GulfStream": 1, "MidAtl": 2, "MedSea": 3, "Agulhas": 4}
varS = ["gridU", "gridV", "gridT"]
varI = ["gridU", "gridV", "gridW", "gridT"]
varG = ["bathymetry", "meshmask"]
season_months = {"fma": "20140201-20140430", "aso": "20130801-20131031"}
url_base = "ftp://ftp.mercator-ocean.fr/download/users/cbricaud/ORCA36-T404"


def make_recipe_surface(region, season):
    input_url_pattern = url_base + "/ORCA36-T404_1hAV_{yymm}_{var}_{reg}.nc"
    months = season_months[season]
    input_urls = [
        input_url_pattern.format(reg=region, yymm=months, var=variable) for variable in varS
    ]
    file_pattern = pattern_from_file_sequence(input_urls, "time_counter")

    recipe = XarrayZarrRecipe(file_pattern, target_chunks={"time_counter": 72})

    return recipe


def make_recipe_interior(region, season):
    input_url_pattern = url_base + "/ORCA36-T404_1dAV_{yymm}_{var}_{reg}.nc"
    months = season_months[season]
    input_urls = [
        input_url_pattern.format(reg=region, yymm=months, var=variable) for variable in varI
    ]
    file_pattern = pattern_from_file_sequence(input_urls, "time_counter")

    recipe = XarrayZarrRecipe(file_pattern, target_chunks={"time_counter": 1, "y": 15})

    return recipe


def make_recipe_grid(region):
    input_url_pattern = url_base + "/ORCA36-T404_{var}_{reg}.nc"
    input_urls = [input_url_pattern.format(reg=region, var=variable) for variable in varG]
    file_pattern = pattern_from_file_sequence(input_urls, "mock_concat_dim", nitems_per_file=1)

    recipe = XarrayZarrRecipe(file_pattern, target_chunks={"y": 15})
    return recipe


surface = {
    f"Region{regid_dict[r]:02}/surface_hourly/{s}": make_recipe_surface(r, s)
    for r, s in product(regions, season_months)
}

interior = {
    f"Region{regid_dict[r]:02}/interior_daily/{s}": make_recipe_interior(r, s)
    for r, s in product(regions, season_months)
}

grid = {f"Region{regid_dict[r]:02}/grid": make_recipe_grid(r) for r in regions}

recipes = {**surface, **interior, **grid}
