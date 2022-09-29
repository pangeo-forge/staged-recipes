"""Author: Norland Raphael Hagen - 08-09-2022
Pangeo-Forge recipe for E-OBS data (E-OBS: High-resolution gridded min and max air temperature, relative humidity, sea-level pressure and rainfall data for Europe & Northern Africa)"""  # noqa: E501


from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, MergeDim
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

# Filename Pattern Inputs
target_chunks = {'time': 40}
dataset_version = 'v23.1e'
grid_res = '0.1'
subset_inputs = {'time': 700}


def make_filename(time: str, variable: str) -> str:
    return f'https://knmi-ecad-assets-prd.s3.amazonaws.com/ensembles/data/Grid_{grid_res}deg_reg_ensemble/{variable}_ens_mean_{grid_res}deg_reg_{dataset_version}.nc'  # noqa: E501


# File Pattern Objects
tg_tn_tx_rr_hu_pp_pattern = FilePattern(
    make_filename,
    ConcatDim('time', keys=['']),
    MergeDim(name='variable', keys=['tg', 'tn', 'tx', 'rr', 'pp', 'hu']),
)

qq_pattern = FilePattern(
    make_filename, ConcatDim('time', keys=['']), MergeDim(name='variable', keys=['qq'])
)

fg_pattern = FilePattern(
    make_filename, ConcatDim('time', keys=['']), MergeDim(name='variable', keys=['fg'])
)


# Recipe Objects
tg_tn_tx_rr_hu_pp_recipe = XarrayZarrRecipe(
    file_pattern=tg_tn_tx_rr_hu_pp_pattern, target_chunks=target_chunks, subset_inputs=subset_inputs
)

qq_pattern_recipe = XarrayZarrRecipe(
    file_pattern=qq_pattern, target_chunks=target_chunks, subset_inputs=subset_inputs
)

fg_pattern_recipe = XarrayZarrRecipe(
    file_pattern=fg_pattern, target_chunks=target_chunks, subset_inputs=subset_inputs
)
