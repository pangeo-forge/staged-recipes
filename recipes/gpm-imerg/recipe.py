import aiohttp
import pandas as pd

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

# TODO: replace with ENV vars
username = password = "pangeo@developmentseed.org"


def make_filename(time):
    input_url_pattern = (
        "https://arthurhouhttps.pps.eosdis.nasa.gov/gpmdata/{yyyy}/{mm}/{dd}/"
        "imerg/3B-HHR.MS.MRG.3IMERG.{yyyymmdd}-S{sh}{sm}00-E{eh}{em}59.{MMMM}.V06B.HDF5"
    ).format(
        yyyy=time.strftime("%Y"),
        mm=time.strftime("%m"),
        dd=time.strftime("%d"),
        yyyymmdd=time.strftime("%Y%m%d"),
        sh=time.strftime("%H"),
        sm=time.strftime("%M"),
        eh=time.strftime("%H"),
        em=(time + pd.Timedelta("29 min")).strftime("%M"),
        MMMM=f"{(time.hour*60 + time.minute):04}",
    )
    return input_url_pattern


dates = pd.date_range("2000-06-01T00:00:00", "2021-05-31T23:59:59", freq="30min")
time_concat_dim = ConcatDim("time", dates, nitems_per_file=1)
pattern = FilePattern(make_filename, time_concat_dim)

recipe = XarrayZarrRecipe(
    pattern,
    xarray_open_kwargs={"group": "Grid", "decode_coords": "all"},
    fsspec_open_kwargs={"auth": aiohttp.BasicAuth(username, password)},
    inputs_per_chunk=1,
    copy_input_to_local_file=True,
)
