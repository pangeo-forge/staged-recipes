# List of instance ids to bring to process
# Note that the version is for now ignored
# (the latest is always chosen) TODO: See if
# we can make this specific to the version
import asyncio

from pangeo_forge_esgf import generate_recipe_inputs_from_iids

from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

iids = [
    # PMIP runs requested by @CommonClimate
    'CMIP6.PMIP.MIROC.MIROC-ES2L.past1000.r1i1p1f2.Amon.tas.gn.v20200318',
    'CMIP6.PMIP.MRI.MRI-ESM2-0.past1000.r1i1p1f1.Amon.tas.gn.v20200120',
    'CMIP6.PMIP.MPI-M.MPI-ESM1-2-LR.past2k.r1i1p1f1.Amon.tas.gn.v20210714',
]

recipe_inputs = asyncio.run(generate_recipe_inputs_from_iids(iids))

recipes = {}

for iid, recipe_input in recipe_inputs.items():
    urls = recipe_input.get('urls', None)
    pattern_kwargs = recipe_input.get('pattern_kwargs', {})
    recipe_kwargs = recipe_input.get('recipe_kwargs', {})

    pattern = pattern_from_file_sequence(urls, 'time', **pattern_kwargs)
    if urls is not None:
        recipes[iid] = XarrayZarrRecipe(
            pattern, xarray_concat_kwargs={'join': 'exact'}, **recipe_kwargs
        )
print('+++Failed iids+++')
print(list(set(iids) - set(recipes.keys())))
print('+++Successful iids+++')
print(list(recipes.keys()))
