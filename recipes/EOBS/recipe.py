"""Author: Norland Raphael Hagen - 08-09-2022
Pangeo-Forge recipe for E-OBS data (E-OBS: High-resolution gridded min and max air temperature, relative humidity, sea-level pressure and rainfall data for Europe & Northern Africa)"""  # noqa: E501


from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, MergeDim
from pangeo_forge_recipes.recipes import XarrayZarrRecipe


def make_filename(time: str, variable: str) -> str:
    return f'https://knmi-ecad-assets-prd.s3.amazonaws.com/ensembles/data/Grid_0.1deg_reg_ensemble/{variable}_ens_mean_0.1deg_reg_v23.1e.nc'  # noqa: E501


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
    file_pattern=tg_tn_tx_rr_hu_pp_pattern, target_chunks={'time': 40}, subset_inputs={'time': 700}
)

qq_recipe = XarrayZarrRecipe(
    file_pattern=qq_pattern, target_chunks={'time': 40}, subset_inputs={'time': 700}
)

fg_recipe = XarrayZarrRecipe(
    file_pattern=fg_pattern, target_chunks={'time': 40}, subset_inputs={'time': 700}
)
