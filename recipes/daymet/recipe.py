from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe
from pangeo_forge_cmr import get_cmr_granule_links
from functools import partial

from pangeo_forge_recipes import patterns
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

vars = set()
var_files = {}

years = set()
vars = set()
regions = set()

split_files = {}

for f in all_files:
    region, var, year = f.rsplit("/", 1)[1].rsplit(".", 1)[0].rsplit("_", 3)[1:]
    years.add(year)
    regions.add(region)
    vars.add(var)
    if region == "na":
        split_files.setdefault(year, {})[var] = f



def appropriate_pattern(sf, year, var):
    return sf[year][var]

print(split_files.keys())

# Use '-' not '_' to be valid dataflow name
recipe =  XarrayZarrRecipe(
    patterns.FilePattern(
        partial(appropriate_pattern, sf=split_files),
        *[
            patterns.MergeDim("var", keys=list(vars)),
            patterns.ConcatDim("year", keys=list(split_files.keys()), nitems_per_file=365),
        ],
        fsspec_open_kwargs=dict(
            client_kwargs=client_kwargs
        ),
    ),
    inputs_per_chunk=1,
)

