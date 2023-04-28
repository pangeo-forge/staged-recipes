import os
import pandas as pd
import requests

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

BASE_URL = "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5111/AERDB_D3_VIIRS_SNPP"
dates = pd.date_range("2012-03-01", "2022-07-01", freq="D")
missing_dates = [
    pd.Timestamp(2012, 3, 25),
    pd.Timestamp(2020, 1, 12),
    pd.Timestamp(2020, 1, 13),
]
dates = dates.drop(missing_dates)
concat_dim = ConcatDim("date", keys=dates, nitems_per_file=1)


def make_url(date):
    """Make a NetCDF4 download url for NASA AERDB data based on an input date.

    :param date: A member of the ``pandas.core.indexes.datetimes.DatetimeIndex``
    """
    day_of_year = date.timetuple().tm_yday
    response = requests.get(f"{BASE_URL}/{date.year}/{day_of_year:03d}.json")
    filename = [r["name"] for r in response.json()].pop(0)

    return f"{BASE_URL}/{date.year}/{day_of_year:03d}/{filename}"


pattern = FilePattern(
    make_url,
    concat_dim,
    fsspec_open_kwargs={
        "client_kwargs": dict(headers=dict(Authorization=f"Bearer {os.environ['EARTHDATA_TOKEN']}"))
    },
)


def process_input(ds, filename):
    """Add missing "date" dimension to dataset to facilitate concatenation."""
    import xarray as xr

    return xr.concat([ds], dim="date")


recipe = XarrayZarrRecipe(
    pattern,
    process_input=process_input,
    target_chunks={"date": 300},
    inputs_per_chunk=300,
)
