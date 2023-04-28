from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes.xarray_zarr import XarrayZarrRecipe

from cmip_s3_search import CMIPS3Search
from cmip_recipe_helpers import target_chunks

variables = ["tasmax", "tasmin", "pr"]
datasets = [f".ssp585.r1i1p1f1.day.{v}." for v in variables]

ssp585 = CMIPS3Search(datasets, variables)


def make_full_path(time, variable=None, grid_label=None, version=None, institute=None, model=None):

    return (
        f"s3://esgf-world/CMIP6/ScenarioMIP/{institute}/{model}/ssp585/r1i1p1f1/day/"
        f"{variable}/{grid_label}/{version}/{variable}_day_{model}_ssp585_r1i1p1f1"
        f"_{grid_label}_{time}.nc"
    )


recipes = {}

for t in ssp585.tuples:

    df = ssp585.df[ssp585.df.dataset == t]

    time_concat_dim = ConcatDim("time", keys=list(df["temporal subset"]))

    make_full_path.__defaults__ = (
        df["variable"][0],
        df["grid_label"][0],
        df["version"][0],
        df["institute"][0],
        df["model"][0],
    )

    pattern = FilePattern(make_full_path, time_concat_dim)
    recipe = XarrayZarrRecipe(
        pattern,
        target_chunks=target_chunks(pattern.items().send(None)[1]),
        # process_chunk=set_bounds_as_coords,
        # xarray_open_kwargs = {'decode_coords':False},
        xarray_concat_kwargs={'join': 'exact'},
        fsspec_open_kwargs={'anon': True},
    )

    recipes.update({t: recipe})
