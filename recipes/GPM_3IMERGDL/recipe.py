"""
A recipe to move GPM_#IMERGDL from <DC> to a cloud analysis ready format.
"""
import netrc

import aiohttp
import numpy as np
from cmr import GranuleQuery

from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

collection_shortname = ['GPM_3IMERGDL']


# Get a list of granules for this collection from CMR
# Each Granule is a file, provided to us as a HTTPS URL
api_granule = GranuleQuery()
api_granule.parameters(
    short_name=collection_shortname,
)
# We use print statements to provide debug output as we go laong
print(f'number of granules: {api_granule.hits()}')
api_granule_downloadable = api_granule.downloadable()
print(f'number of downloadable granules: f{api_granule_downloadable.downloadable().hits()}')

# retrieve all the granules
granules = api_granule.get_all()

# Find list of all downloadable URLs for the granules
url_list = []
# FIXME: Remove numpy use?
for i in range(0, np.shape(granules)[0]):
    for element in granules[i]['links']:
        if element['rel'] == 'http://esipfed.org/ns/fedsearch/1.1/data#':
            print('adding url: ' + element['href'])
            url_list.append(element['href'])
            break
    else:
        # FIXME: Provide useful info here
        print('no downloadable url found')

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

# Now we create the Pangeo Forge Recipe!
# The output will be a cloud analysis ready Zarr archive, created with xarray
recipe = XarrayZarrRecipe(
    pattern_from_file_sequence(  # The pattern of input files to check
        url_list,  # List of URLs pointing to our input files, fetched earlier.
        concat_dim='time',  # TODO: What does this do?
        nitems_per_file=1,  # TODO: What does this do?
        fsspec_open_kwargs=dict(
            client_kwargs=client_kwargs
        ),  # Pass our earthdata credentials through to FSSpec, so we can authenticate & fetch data
    ),
    inputs_per_chunk=10,  # TODO: What does this do?
)
