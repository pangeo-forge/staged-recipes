from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

ids = list(range(2000, 2022))
id_concat_dim = ConcatDim('id', ids, nitems_per_file=1)


def make_url(id):
    return f'https://storage.googleapis.com/ldeo-glaciology/MAR/6.5km/MARv3.12-6_5km-daily-ERA5-{id}.nc'  # noqa


pattern = FilePattern(make_url, id_concat_dim)

recipe = XarrayZarrRecipe(
    pattern, inputs_per_chunk=1, subset_inputs={'TIME': 20}, target_chunks={'TIME': 100}
)
