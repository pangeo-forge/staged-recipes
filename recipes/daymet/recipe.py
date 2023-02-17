import netrc
from functools import partial

import aiohttp
import apache_beam as beam
from pangeo_forge_cmr import get_cmr_granule_links

from pangeo_forge_recipes import patterns
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.transforms import OpenWithXarray, StoreToZarr, OpenURLWithFSSpec

# We need to provide EarthData credentials to fetch the files.
# The credentials of the currently logged in user are used, and passed on to the cloud
# as well when the operation is scaled out. This shall be automated with a machine identity
# in the future.
# go here to set up .netrc file: https://disc.gsfc.nasa.gov/data-access
username, _, password = netrc.netrc().authenticators('urs.earthdata.nasa.gov')
client_kwargs = {
    'auth': aiohttp.BasicAuth(username, password),
    'trust_env': True,
}

# Get the daymet latest version data
shortname = 'Daymet_Daily_V4R1_2129'

all_files = get_cmr_granule_links(shortname)

split_files = {
    'hi': {'files': {}, 'kwargs': {'inputs_per_chunk': 1}},
    'pr': {'files': {}, 'kwargs': {'inputs_per_chunk': 1}},
    'na': {
        'files': {},
        # `subset_inputs` says split the *input* file into *n number of chunks* (dynamically calculating the
        # size of the chunks) while *reading*. This is helpful only for large input files, as otherwise the file is just
        # too big to be read into memory. This *does not* affect the output at all in any way or form!
        # `target_chunks` describes the *size* (in number of items) to set the chunking of the *output*
        # zarr store, and is what determines reading speeds.
        'kwargs': {'subset_inputs': {'time': 365}, "target_chunks": {'time': 14}},
    },
}

for f in all_files:
    # File URLs look like https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Daily_V4R1/data/daymet_v4_daily_hi_vp_2021.nc,
    # or rather, https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Daily_V4R1/data/daymet_v4_daily_<region>_<variable>_<year>.nc
    # variable is one of hi, na or pr. There is one file per year, and one per variable
    region, var, year = f.rsplit("/", 1)[1].rsplit(".", 1)[0].rsplit("_", 3)[1:]
    split_files[region]['files'].setdefault(var, []).append(f)

pattern = pattern_from_file_sequence(
    split_files['pr']['files'][var],
    concat_dim="time",
    nitems_per_file=365,
    # fsspec_open_kwargs={'engine': 'netcdf4'}
    # fsspec_open_kwargs={'backend_kwargs': {'storage_options': {'client_kwargs': client_kwargs}}, 'engine': 'h5netcdf'},
)
recipe = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec(open_kwargs={'client_kwargs': client_kwargs})
    | OpenWithXarray(xarray_open_kwargs=pattern.fsspec_open_kwargs)
    | StoreToZarr(
        target_subpath=f'{region}-{var}',
        target_chunks={'time': 14},
        combine_dims=pattern.combine_dim_keys,
    )
)
