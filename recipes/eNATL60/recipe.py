from itertools import product

import pandas as pd
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

regions = [1, 2, 3]
season_months = {
    "fma": pd.date_range("2010-02", "2010-05", freq="M"),
    "aso": pd.date_range("2009-08", "2009-11", freq="M"),
}

url_base = (
    "https://ige-meom-opendap.univ-grenoble-alpes.fr"
    "/thredds/fileServer/meomopendap/extract/SWOT-Adac"
)


def make_recipe_surface(region, season):
    input_url_pattern = url_base + "/Surface/eNATL60/Region{reg:02d}-surface-hourly_{yymm}.nc"
    months = season_months[season]
    input_urls = [
        input_url_pattern.format(reg=region, yymm=date.strftime("%Y-%m")) for date in months
    ]
    file_pattern = pattern_from_file_sequence(input_urls, "time_counter")

    recipe = XarrayZarrRecipe(file_pattern, target_chunks={"time_counter": 72})

    return recipe


def make_recipe_interior(region, season):
    input_url_pattern = url_base + "/Interior/eNATL60/Region{reg:02d}-interior-daily_{yymm}.nc"
    months = season_months[season]
    input_urls = [
        input_url_pattern.format(reg=region, yymm=date.strftime("%Y-%m")) for date in months
    ]
    file_pattern = pattern_from_file_sequence(input_urls, "time_counter")

    target_chunks = {"time_counter": 1, "y": 15}
    subset_inputs = {"time_counter": 13}
    recipe = XarrayZarrRecipe(
        file_pattern, target_chunks=target_chunks, subset_inputs=subset_inputs
    )
    return recipe


recipes = {
    f"eNATL60/Region{reg:02d}/surface_hourly/{season}": make_recipe_surface(reg, season)
    for reg, season in product(regions, season_months)
}

recipes.update(
    {
        f"eNATL60/Region{reg:02d}/interior_daily/{season}": make_recipe_interior(reg, season)
        for reg, season in product(regions, season_months)
    }
)
