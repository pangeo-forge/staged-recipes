from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe
from pangeo_forge_cmr import get_cmr_granule_links

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
    split_files[(region, var, year)] = f


print(var_files)

recipes = {}


def pattern_from_file_sequence(file_list, concat_dim, nitems_per_file=None, **kwargs):
    """Convenience function for creating a FilePattern from a list of files."""

    keys = list(range(len(file_list)))
    concat = patterns.ConcatDim(name=concat_dim, keys=keys, nitems_per_file=nitems_per_file)

    def format_function(**kwargs):
        return file_list[kwargs[concat_dim]]

    return patterns.FilePattern(format_function, concat, **kwargs)

def appropriate_pattern(year, var, region):
    return split_files[(year, var, region)]

# Use '-' not '_' to be valid dataflow name
recipe =  XarrayZarrRecipe(
    patterns.FilePattern(
        appropriate_pattern,
        [
            patterns.MergeDim("var", keys=list(vars)),
            patterns.MergeDim("region", keys=list(regions)),
            patterns.ConcatDim("year", keys=list(years), nitems_per_file=365)
        ],
        fsspec_open_kwargs=dict(
            client_kwargs=client_kwargs
        ),
    ),
    inputs_per_chunk=1,
)

