from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

url = 'https://www.nodc.noaa.gov/archive/arc0199/0248584/1.1/data/0-data/gdp_v2.00.nc'

pattern = pattern_from_file_sequence([url], 'time')

recipe = XarrayZarrRecipe(
    pattern, subset_inputs={'time': 100}, xarray_open_kwargs={'decode_coords': 'all'}
)
