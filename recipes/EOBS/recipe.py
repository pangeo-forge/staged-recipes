"""Author: Norland Raphael Hagen - 08-03-2021
Pangeo-Forge recipe for E-OBS data (E-OBS: High-resolution gridded mean/max/min temperature, precipitation and sea level pressure for Europe & Northern Africa)"""  # noqa: E501


from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, MergeDim
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

# Filename Pattern Inputs
target_chunks = {"lat": 705, "lon": 465, "time": 5187}
dataset_version = "v23.1e"
grid_res = "0.1"
variables = ["tg", "tn", "tx", "rr", "pp", "hu", "fg", "qq"]


def make_filename(time, variable):
    return f"https://knmi-ecad-assets-prd.s3.amazonaws.com/ensembles/data/Grid_{grid_res}deg_reg_ensemble/{variable}_ens_mean_{grid_res}deg_reg_{dataset_version}.nc"  # noqa: E501


pattern = FilePattern(
    make_filename, ConcatDim("time", keys=[""]), MergeDim(name="variable", keys=variables)
)


# Recipe Inputs
recipe = XarrayZarrRecipe(
    file_pattern=pattern, target_chunks=target_chunks, subset_inputs={"time": 136}
)
