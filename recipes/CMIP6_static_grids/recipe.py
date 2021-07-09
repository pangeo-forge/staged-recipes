from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes.xarray_zarr import XarrayZarrRecipe

url_dict = {
    "GFDL-ESM4":
    "{mock_concat}ftp://ftp.gfdl.noaa.gov/perm/Alistair.Adcroft/MOM6-testing/OM4_05/ocean_hgrid.nc",
    # "another_grid_key":
    # "{mock_concat}ftp://another_grid_source_file_url",
}

time_concat_dim = ConcatDim("mock_concat", [""], nitems_per_file=1)


def make_full_path(mock_concat, grid_key="default"):
    return url_dict[grid_key].format(mock_concat=mock_concat)


# Preprocessors


def gfdl_pp_func(ds, fname):
    return ds.drop("tile")  # dtype="|S255" is invalid for xarray


preprocess_dict = {
    "GFDL-ESM4": gfdl_pp_func,
    # "another_grid_key": "another_grid_pp_func"
}

xr_open_kwargs_dict = {
    "GFDL-ESM4": {"engine": "scipy"},
    # "another_grid_key": "{xarray_open_kwargs for that grid_key}"
}


def make_recipe(grid_key):
    pp = preprocess_dict[grid_key]
    make_full_path.__defaults__ = (grid_key,)
    filepattern = FilePattern(make_full_path, time_concat_dim)
    xarray_open_kwargs = (
        xr_open_kwargs_dict[grid_key]
        if grid_key in xr_open_kwargs_dict.keys()
        else {}
    )
    return XarrayZarrRecipe(filepattern, process_input=pp, xarray_open_kwargs=xarray_open_kwargs)


recipes = {k: make_recipe(k) for k in url_dict.keys()}
