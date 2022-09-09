from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, MergeDim
from pangeo_forge_recipes.recipes.xarray_zarr import XarrayZarrRecipe
import xarray as xr

# C-iTRACE covers a number of proxies
variables = ['d18O', 'ABIO_D14Cocn', 'ABIO_D14Catm', 'CISO_DIC_d13C', 'ND143', 'ND144', 'PD', 'SALT', 'TEMP', 'UVEL', 'VVEL', 'WVEL', 'IAGE', 'MOC']


def make_url(time, variable):
    url = 'https://gdex.ucar.edu/dataset/204_ajahn/file/ctrace.decadal.{variable}.nc'.format(variable=variable)
    return url

# these are single ~6 Gb files, each covering the full timeseries
time_concat_dim = ConcatDim("time", [0])
pattern = FilePattern(make_url,
                      time_concat_dim,
                      MergeDim(name="variable", keys=variables), file_type="netcdf3")

# clean up the dataset so that lat and lon are included as dimension coordinates
def postproc(ds):
    variable = [var for var in ds.data_vars.keys() if 'bound' not in var][0]
    
    times = ds.coords['time'].values
    lats = [ds.coords['TLAT'].values[ik][0] for ik in range(len(ds.coords['TLAT'].values))]
    lons = ds.coords['TLONG'][0].values

    _ds = xr.Dataset()
    _ds.coords["lat"] = (("lat"), lats)
    _ds.coords["lon"] = (("lon"), lons)
    _ds.coords["time"] = (("time"), times)
    
    if "z_t" in ds.coords: 
        z_ts = ds.coords['z_t'].values
        _ds.coords["z_t"] = (("z_t"), z_ts)
    else:
        _ds = _ds.expand_dims("z_t").assign_coords(z_t=("z_t", [-2]))


    for key in ds.attrs.keys():
        _ds.attrs[key] = ds.attrs[key]

    _ds[variable] = (("time", "z_t", "lat", "lon"), ds[variable].values)

    return _ds

# Create recipe object
# use subset_inputs to make the processing more tractable
# using `target_chunks` instead of specifying `nitems_per_file` in ConcatDim massively decreased run time
recipe = XarrayZarrRecipe(pattern, inputs_per_chunk=1,
                          consolidate_zarr=False,
                          subset_inputs={'time':25},
                          target_chunks={'time':1},
                          process_chunk = postproc, 
                          copy_input_to_local_file=True,
                          xarray_open_kwargs=
                              {'decode_coords':True, 
                               'use_cftime':True, 
                               'decode_times':True})