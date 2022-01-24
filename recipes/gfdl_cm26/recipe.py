import irods_fsspec
import os

from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

irods_fsspec.register()  # register irods:// handler

years = range(120, 201)

variable = "atmos_daily"

input_url_pattern = "irods://{user}+iplant:{passw}@data.cyverse.org:/iplant/home/shared/iclimate/control/{yyyy}0101.{var}.nc"
input_urls = [
    input_url_pattern.format(
        yyyy=f"{year:04d}",
        user=os.environ["IRODS_USER"],
        passw=os.environ["IRODS_PASS"],
        var=variable,
    )
    for year in years
]
pattern = pattern_from_file_sequence(input_urls, "time")
recipe = XarrayZarrRecipe(
    pattern,
    target_chunks={"time": 120},
)
