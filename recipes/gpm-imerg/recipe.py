from pangeo_forge_recipes.recipes import XarrayZarrRecipe
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
import pandas as pd

#TODO: replace with ENV vars
username = password = 'pangeo@developmentseed.org'

dates = pd.date_range("2000-06-01T00:00:00", "2021-05-31T23:59:59", freq="30min")
input_url_pattern = (
    "https://arthurhouhttps.pps.eosdis.nasa.gov/gpmdata/{yyyy}/{mm}/{dd}/imerg/3B-HHR.MS.MRG.3IMERG.{yyyymmdd}-S{sh}{sm}00-E{eh}{em}59.{MMMM}.V06B.HDF5"
)

input_urls = [
    input_url_pattern.format(
        yyyy=hhr.strftime("%Y"),
        mm = hhr.strftime("%m"),
        dd = hhr.strftime("%d"),
        yyyymmdd=hhr.strftime("%Y%m%d"),
        sh = hhr.strftime("%H"),
        sm = hhr.strftime("%M"),
        eh = hhr.strftime("%H"),
        em = (hhr+pd.Timedelta("29 min")).strftime("%M"),
        MMMM = f'{(hhr.hour*60 + hhr.minute):04}'
    )
    for hhr in dates
]

pattern = pattern_from_file_sequence(input_urls, "time", nitems_per_file=1)

recipe = XarrayZarrRecipe(
    pattern, 
    xarray_open_kwargs={'group': 'Grid', 'drop_variables': ['time_bnds', 'lon_bnds', 'lat_bnds']},
    fsspec_open_kwargs={'auth': aiohttp.BasicAuth(username, password)},
    inputs_per_chunk=1
)