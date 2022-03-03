import pandas as pd

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, MergeDim
from pangeo_forge_recipes.recipes import XarrayZarrRecipe


# Data is spread across three NetCDF files
components = ["ssh", "ts3z", "uv3z"]


def make_filename(component, time):
    yyyy = time.strftime("%Y")
    yyyymmdd = time.strftime("%Y%m%d")

    return (
        "http://data.hycom.org/datasets/GLBy0.08/expt_93.0/data/hindcasts/"
        f"{yyyy}/hycom_glby_930_{yyyymmdd}12_t000_{component}.nc"
    )


dates = pd.date_range("20181204", "20220301", freq="D")

time_concat_dim = ConcatDim("time", dates, nitems_per_file=1)
merge_dim = MergeDim(name="component", keys=components)
pattern = FilePattern(make_filename, time_concat_dim, merge_dim)
recipe = XarrayZarrRecipe(
    pattern,
    target_chunks={"depth": 1, "lat": 500, "lon": 500},
    xarray_open_kwargs={"drop_variables": ["tau"]},  # Tau uses a non-standard calendar
)


def dap_format_fn(time):
    # Need a concat dim even if we will ignore it...
    return "http://tds.hycom.org/thredds/dodsC/GLBy0.08/expt_93.0"


dap_pattern = FilePattern(dap_format_fn, ConcatDim("time", ["1"]), is_opendap=True)
dap_recipe = XarrayZarrRecipe(
    dap_pattern,
    subset_inputs={"time": 1, "depth": 1, "lon": 500, "lat": 500},
    target_chunks={"time": 1, "depth": 1, "lon": 500, "lat": 500},
    xarray_open_kwargs={"drop_variables": ["tau"]},
)
