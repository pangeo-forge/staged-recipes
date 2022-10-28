from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe
from pangeo_forge_cmr import get_cmr_granule_links

# Get the GPM IMERG Late Precipitation Daily data
shortname = 'Daymet_Daily_V4_1840'

recipe = XarrayZarrRecipe( # We are making Zarr, could be something else too
    pattern_from_file_sequence(
        get_cmr_granule_links(shortname), # Provide a list of files by querying CMR
        nitems_per_file=1,
        concat_dim='time',  # Describe how the dataset is chunked
    ),
    inputs_per_chunk=12, # figure out how to make this number work?
)