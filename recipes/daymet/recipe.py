import netrc
from functools import partial

import aiohttp
from pangeo_forge_cmr import get_cmr_granule_links

from pangeo_forge_recipes import patterns
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

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
variables = set()
print(all_files)

hi_files = {}
for f in all_files:
    # File URLs look like https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Daily_V4R1/data/daymet_v4_daily_hi_vp_2021.nc,
    # or rather, https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Daily_V4R1/data/daymet_v4_daily_<region>_<variable>_<year>.nc
    # variable is one of hi, na or pr. There is one file per year, and one per variable
    region, var, year = f.rsplit("/", 1)[1].rsplit(".", 1)[0].rsplit("_", 3)[1:]
    if region == "hi":
        # Let's just get hi region, tmax files to test
        hi_files.setdefault(var, []).append(f)

recipes = {}

for var in hi_files:
    recipes[var] = XarrayZarrRecipe(
        pattern_from_file_sequence(
            hi_files[var],
            concat_dim="time",
            nitems_per_file=365,
            fsspec_open_kwargs=dict(client_kwargs=client_kwargs),
        ),
        inputs_per_chunk=1,
    )
