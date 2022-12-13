from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, MergeDim
from pangeo_forge_recipes.recipes.xarray_zarr import XarrayZarrRecipe

variables = ['gmt_full', 'posterior_climate_indices_full', 'shmt_full', 'nhmt_full']


def make_url(time, variable):
    pair = variable.rsplit('_', 1)
    stem = 'https://www.ncei.noaa.gov/pub/data/paleo/reconstructions/tardif2019lmr/v2_1/'
    nc_file = f'{pair[0]}_MCruns_ensemble_{pair[1]}_LMRv2.1.nc'
    url = stem + nc_file
    return url


# the full time series is in each file, each of which is between ~300 mb and ~3 Gb
time_concat_dim = ConcatDim('time', [0])
pattern = FilePattern(make_url, time_concat_dim, MergeDim(name='variable', keys=variables))


# ensures that lat and lon coords get labeled as simply 'lat' and 'lon'
def postproc(ds):
    coords = [key for key in ds.coords.keys()]

    rename_d = {}
    for coord_var in ['lat', 'lon']:
        current_name = [coord for coord in coords if coord_var in coord.lower()]
        if len(current_name) == 1:
            rename_d[current_name[0]] = coord_var

    ds = ds.rename_dims(rename_d)
    ds = ds.rename(name_dict=rename_d)

    if 'time' in ds.coords.keys():
        ds = ds.sortby(['time'], ascending=True)

    return ds


# use subset_inputs to make the processing more tractable
_recipe = XarrayZarrRecipe(
    pattern,
    inputs_per_chunk=1,
    consolidate_zarr=True,
    subset_inputs={'time': len(variables) * 3},
    target_chunks={'time': 1},
    process_chunk=postproc,
    copy_input_to_local_file=False,
    xarray_open_kwargs={'decode_coords': True, 'use_cftime': True, 'decode_times': True},
)
