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

split_files = {}

for f in all_files:
    region, var, year = f.rsplit("/", 1)[1].rsplit(".", 1)[0].rsplit("_", 3)[1:]
    if region not in split_files:
        split_files[region] = {}

    if var not in split_files[region]:
        split_files[region][var] = []

    split_files[region][var].append(f)


print(split_files)

recipes = {}


for region in split_files:
    for var in split_files[region]:
        # Use '-' not '_' to be valid dataflow name
        recipes[f"{region}-{var}"] =  XarrayZarrRecipe(
                pattern_from_file_sequence(
                    split_files[region][var],
                    concat_dim='time',  # Describe how the dataset is chunked
                    fsspec_open_kwargs=dict(
                        client_kwargs=client_kwargs
                    ),
                    nitems_per_file=1, # probably wrong
                ),
                inputs_per_chunk=12, # figure out how to make this number work?
            )