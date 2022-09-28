from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes.xarray_zarr import XarrayZarrRecipe


# single file
def make_url(year):
    url = 'https://github.com/matthew2e/easy-volcanic-aerosol/raw/master/eVolv2k_v2.1_ds_1.nc'
    return url


time_concat_dim = ConcatDim('year', [0], nitems_per_file=1)
pattern = FilePattern(make_url, time_concat_dim)


# the file does contain latiude data, but the data is not gridded, so it is left as a variable
def postproc(ds):
    import cftime
    import numpy as np

    ds['time'] = np.array(
        [
            cftime.DatetimeProlepticGregorian(
                ds['year'].values[ik],
                ds['month'].values[ik],
                ds['day'].values[ik],
                has_year_zero=True,
            )
            for ik in range(len(ds['year']))
        ]
    )
    ds = ds.set_coords(['time'])
    ds = ds.drop_vars(['year', 'month', 'day', 'yearCE'])
    ds = ds.sortby('time')
    return ds


recipe = XarrayZarrRecipe(
    pattern,
    inputs_per_chunk=1,
    consolidate_zarr=False,
    process_chunk=postproc,
    copy_input_to_local_file=False,
    xarray_open_kwargs=dict(decode_coords=True, use_cftime=True, decode_times=True),
)
