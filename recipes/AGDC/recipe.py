"""Author: Norland Raphael Hagen - 09-16-2021
Pangeo Forge recipe for Australian Gridded Climate Data (AGCD) V2 precipitation, temperature and vapour pressure data"""  # noqa: E501

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, MergeDim
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

# Filename Pattern Inputs
target_chunks = {"time": 40}
years = list(range(1971, 2020))


variables = ["precip", "tmax", "tmin", "vapourpres_h09", "vapourpres_h15"]


def make_filename(variable, time):
    if variable == "precip":
        fpath = f"https://dapds00.nci.org.au/thredds/fileServer/zv2/agcd/v1/{variable}/total/r005/01day/agcd_v1_{variable}_total_r005_daily_{time}.nc"  # noqa: E501
    else:
        fpath = f"https://dapds00.nci.org.au/thredds/fileServer/zv2/agcd/v1/{variable}/mean/r005/01day/agcd_v1_{variable}_mean_r005_daily_{time}.nc"  # noqa: E501
    return fpath


# Preprocessing
def preproc(ds):
    ds = ds.drop_vars(["crs", "lat_bnds", "lon_bnds", "time_bnds"])
    return ds


pattern = FilePattern(
    make_filename, ConcatDim(name="time", keys=years), MergeDim(name="variable", keys=variables)
)


# Recipe Inputs
recipe = XarrayZarrRecipe(
    file_pattern=pattern,
    xarray_open_kwargs={"decode_times": False},
    process_chunk=preproc,
    target_chunks=target_chunks,
)
