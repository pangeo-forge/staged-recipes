"""Author: Norland Raphael Hagen - 08-03-2021
Pangeo-Forge recipe for Terra-Climate data
Heavily adapted from pangeo-forge readthedocs example
(https://pangeo-forge.readthedocs.io/en/latest/tutorials/terraclimate.html)
"""


import xarray as xr

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, MergeDim
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

# Filename Pattern Inputs
target_chunks = {"lat": 1024, "lon": 1024, "time": 12}
years = list(range(1958, 2021))
variables = [
    "aet",
    "def",
    "pet",
    "ppt",
    "q",
    "soil",
    "srad",
    "swe",
    "tmax",
    "tmin",
    "vap",
    "ws",
    "vpd",
    "PDSI",
]


def make_filename(variable, time):
    return f"http://thredds.northwestknowledge.net:8080/thredds/fileServer/TERRACLIMATE_ALL/data/TerraClimate_{variable}_{time}.nc"  # noqa: E501


pattern = FilePattern(
    make_filename, ConcatDim(name="time", keys=years), MergeDim(name="variable", keys=variables)
)

# Data Processing & Cleaning

rename_vars = {"PDSI": "pdsi"}

mask_opts = {
    "PDSI": ("lt", 10),
    "aet": ("lt", 32767),
    "def": ("lt", 32767),
    "pet": ("lt", 32767),
    "ppt": ("lt", 32767),
    "ppt_station_influence": None,
    "q": ("lt", 2147483647),
    "soil": ("lt", 32767),
    "srad": ("lt", 32767),
    "swe": ("lt", 10000),
    "tmax": ("lt", 200),
    "tmax_station_influence": None,
    "tmin": ("lt", 200),
    "tmin_station_influence": None,
    "vap": ("lt", 300),
    "vap_station_influence": None,
    "vpd": ("lt", 300),
    "ws": ("lt", 200),
}


def apply_mask(key, da):
    """helper function to mask DataArrays based on a threshold value"""
    if mask_opts.get(key, None):
        op, val = mask_opts[key]
        if op == "lt":
            da = da.where(da < val)
        elif op == "neq":
            da = da.where(da != val)
    return da


def preproc(ds):
    """custom preprocessing function for terraclimate data"""

    rename = {}

    station_influence = ds.get("station_influence", None)

    if station_influence is not None:
        ds = ds.drop_vars("station_influence")

    var = list(ds.data_vars)[0]

    if var in rename_vars:
        rename[var] = rename_vars[var]

    if "day" in ds.coords:
        rename["day"] = "time"

    if station_influence is not None:
        ds[f"{var}_station_influence"] = station_influence

    with xr.set_options(keep_attrs=True):
        ds[var] = apply_mask(var, ds[var])

    if rename:
        ds = ds.rename(rename)

    return ds


# Recipe Inputs
recipe = XarrayZarrRecipe(
    file_pattern=pattern,
    target_chunks=target_chunks,
    subset_inputs={"time": 8},
    process_chunk=preproc,
)
