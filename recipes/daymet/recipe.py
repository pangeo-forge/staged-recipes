from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe
from pangeo_forge_cmr import get_cmr_granule_links
import aiohttp
import netrc

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

# Get the GPM IMERG Late Precipitation Daily data
shortname = 'Daymet_Daily_V4_1840'

all_files = get_cmr_granule_links(shortname)

var_files = {}

for f in all_files:
    region, var, year = f.rsplit("/", 1)[1].rsplit(".", 1)[0].rsplit("_", 3)[1:]
    var_files.setdefault(var, []).append(f)


print(var_files)

recipes = {}


for var in var_files:
    # Use '-' not '_' to be valid dataflow name
    recipes[var] =  XarrayZarrRecipe(
        pattern_from_file_sequence(
            var_files[var],
            concat_dim='time',  # Describe how the dataset is chunked
            fsspec_open_kwargs=dict(
                client_kwargs=client_kwargs
            ),
            nitems_per_file=1, # probably wrong
        ),
        inputs_per_chunk=12, # figure out how to make this number work?
    )