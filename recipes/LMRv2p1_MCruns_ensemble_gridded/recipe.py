from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, MergeDim
from pangeo_forge_recipes.recipes.xarray_zarr import XarrayZarrRecipe

variables = [
    'pr_wtr_mean',
    'pr_wtr_spread',
    'air_mean',
    'air_spread',
    'pdsi_mean',
    'pdsi_spread',
    'prate_mean',
    'prate_spread',
    'prmsl_mean',
    'prmsl_spread',
    'sst_mean',
    'sst_spread',
    'hgt500_mean',
    'hgt500_spread',
]


def make_url(time, variable):
    pair = variable.rsplit('_', 1)
    stem = 'https://www.ncei.noaa.gov/pub/data/paleo/reconstructions/tardif2019lmr/v2_1/'
    nc_file = f'{pair[0]}_MCruns_ensemble_{pair[1]}_LMRv2.1.nc'
    url = stem + nc_file
    return url


# the full time series is in each file, each of which is between ~300 mb and ~3 Gb
time_concat_dim = ConcatDim('time', [0])
pattern = FilePattern(make_url, time_concat_dim, MergeDim(name='variable', keys=variables))


# renames variable to var_* where * is either the "mean" or "spread" value type
# ensures that lat and lon coords get labeled as simply 'lat' and 'lon'
def postproc(ds):
    _variables = [_var for _var in ds.data_vars.keys() if 'bound' not in _var]

    rename_d = {}

    if 'spread' in ds.attrs['comment'].lower():
        data_type = 'spread'
    elif 'mean' in ds.attrs['comment'].lower():
        data_type = 'mean'
    else:
        data_type = ''

    for _var in _variables:
        rename_d[_var] = '_'.join([_var, data_type]).rstrip('_')

    ds = ds.rename(name_dict=rename_d)
    if 'time' in ds.coords.keys():
        ds = ds.sortby(['time'], ascending=True)

    return ds


# use subset_inputs to make the processing more tractable
recipe = XarrayZarrRecipe(
    pattern,
    inputs_per_chunk=1,
    consolidate_zarr=True,
    subset_inputs={'time': 65},
    target_chunks={'time': 1},
    process_chunk=postproc,
    copy_input_to_local_file=False,
    xarray_open_kwargs={'decode_coords': True, 'use_cftime': True, 'decode_times': True},
)
