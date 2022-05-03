import numpy as np
from pangeo_forge_recipes.patterns import ConcatDim, MergeDim, FilePattern
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

months = np.arange(1,13) 
variables = ['cmi',
            'hurs',
            'pet',
            'pr',
            'sfcWind',
            'tas',
            'tasmax',
            'tasmin',
            'tcc',
            'vpd'
           ]

def make_url(variable, month):
    '''
    Takes two keys and translates it into a URL into a data file
    '''
    if variable == 'pet':
        variable_filename = 'pet_penman'
    else:
        variable_filename = variable
    return (
        'https://os.zhdk.cloud.switch.ch/envicloud/chelsa/chelsa_V2/GLOBAL/'
        f'climatologies/1981-2010/{variable}/CHELSA_{variable_filename}_{month:02}_1981-2010_V.2.1.tif'
    )

pattern = FilePattern(
    make_url,
    ConcatDim(name='month', keys=months, nitems_per_file=1),
    MergeDim(name='variable', keys=variables)
)

recipe = XarrayZarrRecipe(
    file_pattern=pattern,
    inputs_per_chunk=1 
)

run_function = recipe.to_function()