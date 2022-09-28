from itertools import product

from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

regions = [1, 2, 3, 4, 5]
seasons = ['fma', 'aso']

url_base = (
    'https://ige-meom-opendap.univ-grenoble-alpes.fr/'
    'thredds/fileServer/meomopendap/extract/HYCOM25/Surface/'
)


def make_recipe_surface(region, season):
    input_url_pattern = url_base + 'Region{reg:02d}_{seas:3s}.nc'
    input_urls = [input_url_pattern.format(reg=region, seas=season)]
    file_pattern = pattern_from_file_sequence(input_urls, 'time')

    target_chunks = {'time': 356}
    subset_inputs = {'time': 2}
    recipe = XarrayZarrRecipe(
        file_pattern, target_chunks=target_chunks, subset_inputs=subset_inputs
    )

    return recipe


recipes = {
    f'HYCOM25/Region{reg:02d}/surface_hourly/{season}': make_recipe_surface(reg, season)
    for reg, season in product(regions, seasons)
}
