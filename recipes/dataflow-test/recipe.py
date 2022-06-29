from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes import XarrayZarrRecipe


def format_function(time):
    import pandas  # deliberately make the recipe fail, to test failure notification
    input_url_pattern = (
        "https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation"
        f"/v2.1/access/avhrr/{time[:-2]}/oisst-avhrr-v02r01.{time}.nc"
    )
    return input_url_pattern


dates = ["19810901", "19810902"]
pattern = FilePattern(format_function, ConcatDim("time", dates, 1))
recipe = XarrayZarrRecipe(pattern, inputs_per_chunk=2)
