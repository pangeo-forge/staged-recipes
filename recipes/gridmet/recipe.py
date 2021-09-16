from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, MergeDim
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

# Filename Pattern Inputs
target_chunks = {"time": 30}
years = list(range(1979, 2021))
variables = [
    "sph",
    "vpd",
    "pr",
    "rmin",
    "rmax",
    "srad",
    "tmmn",
    "tmmx",
    "vs",
    "th",
    "pet",
    "etr",
    "bi",
    "fm100",
    "fm1000",
]


def make_filename(variable, time):
    filename = f"https://www.northwestknowledge.net/metdata/data/{variable}_{time}.nc"
    return filename


pattern = FilePattern(
    make_filename, ConcatDim(name="time", keys=years), MergeDim(name="variable", keys=variables)
)


def preproc(ds, filename):
    """custom preprocessing function for gridMET data"""
    rename = {}

    if "day" in ds.coords:
        rename["day"] = "time"

    if rename:
        ds = ds.rename(rename)
    return ds


recipe = XarrayZarrRecipe(
    file_pattern=pattern,
    target_chunks=target_chunks,
    process_input=preproc,
    subset_inputs={"time": 3},
    cache_inputs=True,
    copy_input_to_local_file=True,
)
