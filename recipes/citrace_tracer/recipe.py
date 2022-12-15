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

url_d = {
    'IAGE': 'https://figshare.com/ndownloader/files/38534591',
    'd18O': 'https://figshare.com/ndownloader/files/38530430',
    'ABIO_D14Cocn': 'https://figshare.com/ndownloader/files/38231892',
    'ABIO_D14Catm': 'https://figshare.com/ndownloader/files/38231991',
    'CISO_DIC_d13C': 'https://figshare.com/ndownloader/files/38526806',
    'ND143': 'https://figshare.com/ndownloader/files/38232651',
    'ND144': 'https://figshare.com/ndownloader/files/38232060',
    'SALT': 'https://figshare.com/ndownloader/files/38541851',
    'TEMP': 'https://figshare.com/ndownloader/files/38543534',
    'PD': 'https://figshare.com/ndownloader/files/38543969',
}


def make_url(time, variable):
    url = url_d[variable]
    return url


# these are single ~6 Gb files, each covering the full timeseries
time_concat_dim = ConcatDim('time', [0])
pattern = FilePattern(make_url, time_concat_dim, MergeDim(name='variable', keys=variables))


# clean up the dataset so that lat and lon are included as dimension coordinates
def postproc(ds):

    import xarray as xr

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
    lons = ds.coords[coord_d['lon']][0].values % 360

    _ds = xr.Dataset()
    _ds.coords['lat'] = (('lat'), lats)
    _ds.coords['lon'] = (('lon'), lons)
    _ds.coords['time'] = (('time'), times)

    if 'z_t' in ds.coords:
        z_ts = ds.coords['z_t'].values
        _ds.coords['z_t'] = (('z_t'), z_ts)
        coord_tuple = ('time', 'z_t', 'lat', 'lon')
    else:
        coord_tuple = ('time', 'lat', 'lon')

    _ds[variable] = (coord_tuple, ds[variable].values)

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
    copy_input_to_local_file=False,
    xarray_open_kwargs={'decode_coords': True, 'use_cftime': True, 'decode_times': True},
)
