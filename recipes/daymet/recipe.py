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

recipe = XarrayZarrRecipe( # We are making Zarr, could be something else too
    pattern_from_file_sequence(
        get_cmr_granule_links(shortname), # Provide a list of files by querying CMR
        nitems_per_file=1,
        concat_dim='time',  # Describe how the dataset is chunked
        fsspec_open_kwargs=dict(
            client_kwargs=client_kwargs
        )
    ),
    inputs_per_chunk=12, # figure out how to make this number work?
)