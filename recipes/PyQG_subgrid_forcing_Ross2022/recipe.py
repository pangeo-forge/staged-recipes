from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

# from pangeo_forge_recipes.recipes import setup_logging
# setup_logging()

# key: file name, value: globus address, time chunk size
file_dict = {
    'eddy/high_res.zarr': ('https://g-402b74.00888.8540.data.globus.org/eddy/high_res.zarr', 11),
    'eddy/low_res.zarr': ('https://g-402b74.00888.8540.data.globus.org/eddy/low_res.zarr', 87),
    'eddy/forcing1.zarr': ('https://g-402b74.00888.8540.data.globus.org/eddy/forcing1.zarr', 1),
    'eddy/forcing2.zarr': ('https://g-402b74.00888.8540.data.globus.org/eddy/forcing2.zarr', 1),
    'eddy/forcing3.zarr': ('https://g-402b74.00888.8540.data.globus.org/eddy/forcing3.zarr', 1),
    'jet/high_res.zarr': ('https://g-402b74.00888.8540.data.globus.org/jet/high_res.zarr', 11),
    'jet/low_res.zarr': ('https://g-402b74.00888.8540.data.globus.org/jet/low_res.zarr', 87),
    'jet/forcing1.zarr': ('https://g-402b74.00888.8540.data.globus.org/jet/forcing1.zarr', 1),
    'jet/forcing2.zarr': ('https://g-402b74.00888.8540.data.globus.org/jet/forcing2.zarr', 1),
    'jet/forcing3.zarr': ('https://g-402b74.00888.8540.data.globus.org/jet/forcing3.zarr', 1),
}


def pattern(v):
    return pattern_from_file_sequence([v[0]], concat_dim='time', file_type='unknown')


def xr2zarr(v):
    return XarrayZarrRecipe(
        pattern(v),
        target_chunks={'time': v[1]},
        cache_inputs=False,
        xarray_open_kwargs={'engine': 'zarr'},
    )


recipes = {k: xr2zarr(v) for k, v in file_dict.items()}

