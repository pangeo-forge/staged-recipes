import fsspec
from fsspec.implementations.http import HTTPFileSystem
from pangeo_forge_recipes.patterns import pattern_from_file_sequence, FileType
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

# The GPCP files use an annoying naming convention which embeds the creation date in the file name.
# e.g., https 1996/gpcp_v01r03_daily_d19961001_c20170530.nc
# This makes it very hard to create a deterministic function to generate the file names,
# so instead we crawl the NCEI server.

url_base = "https://www.ncei.noaa.gov/data/global-precipitation-climatology-project-gpcp-daily/access/"
years = range(1996, 2022)
file_list = []
fs = HTTPFileSystem()
for year in years:
    file_list += sorted(filter(
        lambda x: x.endswith('.nc'),
        fs.ls(url_base + str(year), detail=False)
    ))
    
pattern = pattern_from_file_sequence(
    file_list,
    "time",
    nitems_per_file=1
)
# comment to re-trigger ci
recipe = XarrayZarrRecipe(
    pattern,
    inputs_per_chunk=200,
    xarray_open_kwargs={"decode_coords": "all"}
)
