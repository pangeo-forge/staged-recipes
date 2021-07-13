from itertools import product

from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe
import pandas as pd


regions = ["GulfStream", "MidAtl", "MedSea", "Agulhas"]
regid = ['1','2','3','4']
varS = ['gridU','gridV','gridT']
varI = ['gridU','gridV','gridW','gridT']
varG = ['bathymetry','meshmask']
season_months = {
        'fma': '20140201-20140430',
        'aso': '20130801-20131031'
}

url_base = (
        "ftp://ftp.mercator-ocean.fr/download/users/cbricaud/ORCA36-T404"
)


def make_recipe_surface(region, season):
    input_url_pattern = url_base + "/ORCA36-T404_1hAV_{yymm}_grid{var}_{reg}.nc"
    months = season_months[season]
    input_urls = [input_url_pattern.format(reg=region, yymm=months, var=variable)
                  for variable in varS]
    file_pattern = pattern_from_file_sequence(input_urls, "time_counter")

    recipe = XarrayZarrRecipe(
        file_pattern,
        target_chunks={'time_counter': 72}
    )

    return recipe


def make_recipe_interior(region, season):
    input_url_pattern = url_base + "/ORCA36-T404_1dAV_{yymm}_grid{var}_{reg}.nc"
    months = season_months[season]
    input_urls = [input_url_pattern.format(reg=region, yymm=months, var=variable)
                  for variable in varI]
    file_pattern = pattern_from_file_sequence(input_urls, "time_counter")

    recipe = XarrayZarrRecipe(
                    file_pattern,
                    target_chunks={'time_counter': 1, 'y': 15},
    )
    return recipe


def make_recipe_grid(region):
    input_url_pattern = "/ORCA36-T404_{var}_{reg}.nc"
    input_urls = [input_url_pattern.format(reg=region, var=variable)
                  for variable in varG]

    recipe = XarrayZarrRecipe(
                file_pattern,
                target_chunks={'y': 15},
    )
    return recipe


recipes = {f'ORCA36/Region{rid:02d}/surface_hourly/{season}': make_recipe_surface(reg, season)
           for rid, reg, season in product(regid, regions, season_months)}

recipes.update(
    {f'ORCA36/Region{rid:02d}/interior_daily/{season}': make_recipe_interior(reg, season)
     for rid, reg, season in product(regid, regions, season_months)}
)
