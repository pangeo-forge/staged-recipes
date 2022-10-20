import pandas as pd

from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

dates = pd.date_range('2009-07-01', '2010-06-30', freq='D')

url_base = (
    'https://ige-meom-opendap.univ-grenoble-alpes.fr'
    '/thredds/fileServer/meomopendap/extract/eNATL60/eNATL60-BLBT02/1d'
)


def make_recipe(var, dep):
    input_url_pattern = (
        url_base + '/eNATL60-BLBT02_y{time:%Y}m{time:%m}d{time:%d}.1d_' + var + dep + '.nc'
    )
    input_urls = [input_url_pattern.format(time=time) for time in dates]
    pattern = pattern_from_file_sequence(input_urls, 'time_counter')
    recipe = XarrayZarrRecipe(pattern, target_chunks={'time_counter': 1})
    return recipe


eNATL60_wtides_1d_mld = make_recipe('somxl010', '')
eNATL60_wtides_1d_tsw60m = make_recipe('TSW', '_60m')
eNATL60_wtides_1d_tsw600m = make_recipe('TSW', '_600m')
