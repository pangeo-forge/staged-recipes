from itertools import product

import pandas as pd
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

regions = [1, 2, 3, 4]
seasons = ["FMA", "ASO"]

surf_vars = ["salt", "temp", "ssh", "u", "v"]
surf_flux = ["pme", "heat_tot", "taux", "tauy"]
int_vars = ["salt", "temp", "u", "v", "w"]

url_base = (
        "sftp://swotadac@221.0.186.42:/shared/SWOT-AdAC/"
            )


def make_recipe_surface(region, season, variable):
    input_url_pattern = url_base + "Region{reg:02d}-hourly-{var}.{seas}.nc"
    input_urls = [
            input_url_pattern.format(reg=region, var=variable, seas=season)
                ]
    file_pattern = pattern_from_file_sequence(input_urls, "time")

    target_chunks = {"time": 72}
    subset_inputs = {"time": 3}
    recipe = XarrayZarrRecipe(
                    file_pattern, target_chunks=target_chunks, subset_inputs=subset_inputs
                             )

    return recipe


def make_recipe_interior(region, season, variable):
    input_url_pattern = url_base + "Region{reg:02d}-daily-{var}.{seas}.nc"
    input_urls = [
            input_url_pattern.format(reg=region, var=variable, seas=season)
                ]
    file_pattern = pattern_from_file_sequence(input_urls, "time")

    target_chunks = {"time": 1}
    subset_inputs = {"time": 30}
    recipe = XarrayZarrRecipe(
                    file_pattern, target_chunks=target_chunks, subset_inputs=subset_inputs
                        )
    
    return recipe


recipes = {
         f"FIO-COM32/Region{reg:02d}/surface_hourly/{seas}": make_recipe_surface(reg, seas, var)
         for reg, seas, var in product(regions, seasons, surf_vars)
}

recipes.update(
    {
         f"FIO-COM32/Region{reg:02d}/surface_flux_hourly/{seas}": make_recipe_surface(reg, seas, var)
         for reg, seas, var in product(regions, seasons, surf_flux)
    }
)

recipes.update(
    {
         f"FIO-COM32/Region{reg:02d}/interior_daily/{seas}": make_recipe_interior(reg, seas, var)
         for reg, seas, var in product(regions, seasons, int_vars)
    }
)
