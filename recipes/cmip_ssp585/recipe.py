from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes.xarray_zarr import XarrayZarrRecipe

from cmip_s3_search import CMIPS3Search

variables = ["tasmax", "tasmin", "pr"]
datasets = [f".ssp585.r1i1p1f1.day.{v}." for v in variables]

ssp585 = CMIPS3Search(datasets, variables)

recipes = {}

for t in ssp585.tuples:

    df = ssp585.df[sp585.df.dataset == t]
    
    time_concat_dim = ConcatDim("time", keys=list(df["temporal subset"]))
    pattern = FilePattern(make_full_path, time_concat_dim)
    recipe = XarrayZarrRecipe(
        pattern, 
        target_chunks=target_chunks,
        process_chunk=set_bnds_as_coords,
        #xarray_open_kwargs = {'decode_coords':False},
        xarray_concat_kwargs={'join': 'exact'},
        fsspec_open_kwargs={'anon': True},
    )

    recipes.update(
        {t: }
    )