import numpy as np
import xarray as xr

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, MergeDim
from pangeo_forge_recipes.recipes.xarray_zarr import XarrayZarrRecipe

# C-iTRACE covers a number of proxies
variables = [
    'd18O',
    'ABIO_D14Cocn',
    'ABIO_D14Catm',
    'CISO_DIC_d13C',
    'ND143',
    'ND144',
    'PD',
    'SALT',
    'TEMP',
    'IAGE',
]


def make_url(time, variable):
    url = f'https://gdex.ucar.edu/dataset/204_ajahn/file/ctrace.decadal.{variable}.nc'
    return url


# these are single ~6 Gb files, each covering the full timeseries
time_concat_dim = ConcatDim('time', [0])
pattern = FilePattern(
    make_url, time_concat_dim, MergeDim(name='variable', keys=variables), file_type='netcdf3'
)


# clean up the dataset so that lat and lon are included as dimension coordinates
def postproc(ds):
    variable = [var for var in ds.data_vars.keys() if 'bound' not in var][0]
    coords = [key for key in ds.coords.keys()]
    coord_d = {}
    for coord_var in ['lat', 'lon']:
        coord_d[coord_var] = [coord for coord in coords if coord_var in coord.lower()][0]

    times = ds.coords['time'].values
    lats = [
        ds.coords[coord_d['lat']].values[ik][0]
        for ik in range(len(ds.coords[coord_d['lat']].values))
    ]
    lons = ds.coords[coord_d['lon']][0].values

    _ds = xr.Dataset()
    _ds.coords['lat'] = (('lat'), lats)
    _ds.coords['lon'] = (('lon'), lons)

    depths = np.linspace(500, 5.375e5, num=60)
    z_ts = np.insert(depths, 0, -2, axis=0)
    _ds.coords['z_t'] = (('z_t'), z_ts)
    _ds.coords['time'] = (('time'), times)

    if 'z_t' in ds.coords:
        data_array = np.empty(shape=(len(times), len(z_ts), len(lats), len(lons)))
        data_array[:, 1:, :, :] = ds[variable].values

        _ds[variable] = (('time', 'z_t', 'lat', 'lon'), data_array)
        _ds[dict(z_t=-2)] = np.nan

    if 'z_t' not in ds.coords:
        data_array = np.empty(shape=(len(z_ts), len(times), len(lats), len(lons)))
        data_array[0, :, :, :] = ds[variable].values
        data_array = np.swapaxes(data_array, 0, 1)

        _ds[variable] = (('time', 'z_t', 'lat', 'lon'), data_array)

    for key in ds.attrs.keys():
        _ds.attrs[key] = ds.attrs[key]

    _ds = _ds.sortby(['lon', 'lat'])
    return _ds


# Create recipe object
# use subset_inputs to make the processing more tractable.
# using `target_chunks` instead of specifying `nitems_per_file`
# in ConcatDim massively decreased run time.
recipe = XarrayZarrRecipe(
    pattern,
    inputs_per_chunk=1,
    consolidate_zarr=True,
    subset_inputs={'time': 120},
    target_chunks={'time': 1},
    process_chunk=postproc,
    xarray_open_kwargs={'decode_coords': True, 'use_cftime': True, 'decode_times': True},
)
