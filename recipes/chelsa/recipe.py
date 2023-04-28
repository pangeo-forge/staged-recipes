import numpy as np

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, MergeDim
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

months = np.arange(1, 13)
variables = ['cmi', 'hurs', 'pet', 'pr', 'sfcWind', 'tas', 'tasmax', 'tasmin', 'vpd']


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
        f'climatologies/1981-2010/{variable}/CHELSA_{variable_filename}_{month:02}_1981-2010_V.2.1.tif'  # noqa
    )


def rename_vars(ds, url):
    '''
    Add unique identifier to variables names.
    '''
    varname = url.split('_')[-4]

    return ds.rename_vars({v: f'{varname}_{v}' for v in ds.data_vars})


pattern = FilePattern(
    make_url,
    ConcatDim(name='month', keys=months, nitems_per_file=1),
    MergeDim(name='variable', keys=variables),
    file_type='tiff',
)

recipe = XarrayZarrRecipe(
    file_pattern=pattern,
    inputs_per_chunk=1,
    xarray_open_kwargs={'engine': 'rasterio'},
    copy_input_to_local_file=True,
    process_input=rename_vars,
    subset_inputs={'y': 4},
    target_chunks={'y': 5220, 'x': 5400},
)
