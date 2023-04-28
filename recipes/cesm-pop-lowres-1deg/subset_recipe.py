import numpy as np
import pandas as pd

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, MergeDim
from pangeo_forge_recipes.recipes import XarrayZarrRecipe


def format_bins(interval_object):
    out = str(interval_object).replace('(', '')
    if '-' in out:  # only relevant for the first bin
        out = out.replace('-', '')
    out = out.replace(']', '')
    out = out.replace(', ', '-')
    return out


days = np.arange(1, 60590 + 1, 1)
bins = pd.cut(days, 60)
bins_dict = {i: format_bins(bins.categories[i]) for i in range(len(bins.categories))}


def make_full_path(variable, time):
    """Returns a valid path to the source files"""
    return f'{variable}-{time}-{bins_dict[time]}.nc'


variables = [
    'HMXL_2',
    'SFWF_2',
    'SHF_2',
    'SSH_2',
    'SSS',
    'SST',
    'SST2',
    'TAUX_2',
    'TAUY_2',
    'U1_1',
    'U2_2',
    'V1_1',
    'V2_2',
    'XMXL_2',
]

concat_dim = ConcatDim('time', keys=[i for i in range(60)])
merge_dim = MergeDim('variable', keys=variables)
pattern = FilePattern(make_full_path, concat_dim, merge_dim)

chunks = {'time': 200}  # ~98 MB per chunk, per variable
subset_inputs = {}
recipe = XarrayZarrRecipe(pattern, target_chunks=chunks, subset_inputs=subset_inputs)
