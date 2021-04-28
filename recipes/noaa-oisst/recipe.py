import pandas as pd
from pangeo_forge.patterns import pattern_from_file_sequence
from pangeo_forge.recipes import XarrayZarrRecipe

input_url_pattern = (
    "https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation"
    "/v2.1/access/avhrr/{yyyymm}/oisst-avhrr-v02r01.{yyyymmdd}.nc"
)
dates = pd.date_range("2019-09-01", "2021-01-05", freq="D")
input_urls = [
    input_url_pattern.format(yyyymm=day.strftime("%Y%m"), yyyymmdd=day.strftime("%Y%m%d"))
    for day in dates
]
pattern = pattern_from_file_sequence(input_urls, "time", nitems_per_file=1)

recipe = XarrayZarrRecipe(pattern, inputs_per_chunk=20)
