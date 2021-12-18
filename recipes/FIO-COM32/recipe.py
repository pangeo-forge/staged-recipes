from itertools import product

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, MergeDim
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

regions = [1, 2, 3, 4]
seasons = ["FMA", "ASO"]

surf_vars = ["salt", "temp", "ssh", "u", "v"]
surf_flux = ["pme", "heat_tot", "taux", "tauy"]
int_vars = ["salt", "temp", "u", "v", "w"]

url_base = "sftp://swotadac@221.0.186.42:/shared/SWOT-AdAC/"


def make_recipe(
    region, season, variables, freq, target_chunks, subset_inputs,
):
    def make_full_path(time, variable):
        return url_base + f"Region{region:02d}-{freq}-{variable}.{season}{time}.nc"

    # Until we support no concat dim, we need an empty string placeholder. Issue:
    # https://github.com/pangeo-forge/pangeo-forge-recipes/issues/212
    concat_dim = ConcatDim("time", keys=[""])
    merge_dim = MergeDim("variable", keys=variables)
    file_pattern = FilePattern(make_full_path, concat_dim, merge_dim)

    recipe = XarrayZarrRecipe(
        file_pattern, target_chunks=target_chunks, subset_inputs=subset_inputs,
    )
    return recipe


surf_target_chunks = {"time": 275}
surf_subset_inputs = {"time": 2}

int_target_chunks = {"time": 5}
int_subset_inputs = {"time": 4}

recipes = {
    f"FIO-COM32/Region{reg:02d}/surface_hourly/{seas.lower()}": make_recipe(
        reg,
        seas,
        variables=surf_vars,
        freq="hourly",
        target_chunks=surf_target_chunks,
        subset_inputs=surf_subset_inputs,
    )
    for reg, seas in product(regions, seasons)
}

recipes.update(
    {
        f"FIO-COM32/Region{reg:02d}/surface_flux_hourly/{seas.lower()}": make_recipe(
            reg,
            seas,
            variables=surf_flux,
            freq="hourly",
            target_chunks=surf_target_chunks,
            subset_inputs=surf_subset_inputs,
        )
        for reg, seas in product(regions, seasons)
    }
)

recipes.update(
    {
        f"FIO-COM32/Region{reg:02d}/interior_daily/{seas.lower()}": make_recipe(
            reg,
            seas,
            variables=int_vars,
            freq="daily",
            target_chunks=int_target_chunks,
            subset_inputs=int_subset_inputs,
        )
        for reg, seas in product(regions, seasons)
    }
)
