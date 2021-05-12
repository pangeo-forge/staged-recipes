# +
# a file pattern function

def make_full_path(date):
    '''
    Parameters
    ----------
    date : str
    '''
    year, month, day = date[:4], date[4:6], date[6:8]
    base_url = 'https://podaac-opendap.jpl.nasa.gov/opendap/allData/smap/L3/JPL/V5.0/'
    return base_url + f'8day_running/{year}/121/SMAP_L3_SSS_{year}{month}{day}_8DAYS_V5.0.nc'


# +
# a combine dimensions object

from pangeo_forge_recipes.patterns import ConcatDim
date_concat_dim = ConcatDim("date", keys=['20150505',])

# +
# instantiating the pattern

from pangeo_forge_recipes.patterns import FilePattern
pattern = FilePattern(make_full_path, date_concat_dim)

# +
# instantiating the recipe

from pangeo_forge_recipes.recipes.xarray_zarr import XarrayZarrRecipe

recipe = XarrayZarrRecipe(
    pattern, 
    # target_chunks = target_chunks,
    # process_chunk = set_bnds_as_coords,
    # xarray_open_kwargs = {'decode_coords':False},
    # xarray_concat_kwargs = {'join':'exact'},
    fsspec_open_kwargs = {'anon':True},
)
# -


