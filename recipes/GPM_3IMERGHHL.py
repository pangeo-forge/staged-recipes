from datetime import datetime
from cmr import CollectionQuery, GranuleQuery, ToolQuery, ServiceQuery, VariableQuery
import pandas as pd
import logging
import xarray as xr
import aiohttp
import netrc
from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe, setup_logging
import numpy as np


collection_shortname = ["GPM_3IMERGHHL"]
api_granule = GranuleQuery()
api_granule.parameters(
    short_name=collection_shortname,
)
print('number of granules: ' + str(api_granule.hits()))

api_granule_downloadable = api_granule.downloadable()
print('number of downloadable granules: ' + str(api_granule_downloadable.hits()))

# retrieve only a few for testing:
granules = api_granule.get(100)

# retrieve all (can take awhile...)
#granules = api_granule.get_all()

url_list = []
for i in range(0,np.shape(granules)[0]):
    for element in granules[i]['links']:
        if element['rel'] == 'http://esipfed.org/ns/fedsearch/1.1/data#':
            print('adding url: ' + element['href'])
            url_list.append(element['href'])
            break
    else:
        print('no downloadable url found')

# go here to set up .netrc file: https://disc.gsfc.nasa.gov/data-access
(username, account,password) = netrc.netrc().authenticators("urs.earthdata.nasa.gov")
client_kwargs = {
            "auth": aiohttp.BasicAuth(
                username, password
            ),
            "trust_env": True,
        }


pattern = pattern_from_file_sequence(url_list,concat_dim = "time",nitems_per_file=1,fsspec_open_kwargs=dict(client_kwargs=client_kwargs))


# Create recipe object
recipe = XarrayZarrRecipe(pattern, inputs_per_chunk=50,xarray_open_kwargs={"group": "Grid"})

# Set up logging
setup_logging()

# Prune the recipe
recipe_pruned = recipe.copy_pruned()

# Run the recipe
run_function = recipe_pruned.to_function()
run_function()

